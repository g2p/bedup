#!/usr/bin/env python
# vim: set fileencoding=utf-8 sw=4 ts=4 et :

import argparse
import collections
import errno
import fcntl
import hashlib
import os
import sqlalchemy
import stat
import sys
import xdg.BaseDirectory  # pyxdg, apt:python-xdg

from .btrfs import (
    lookup_ino_paths, get_fsid, get_root_id,
    get_root_generation, clone_data, defragment, find_new)
from .dedup import ImmutableFDs, cmp_files, dedup_same
from .ioprio import set_idle_priority
from .openat import fopenat, fopenat_rw
from .tracking_model import (
    Filesystem, Inode, Commonality1, Commonality2, get_or_create, META)

from sqlalchemy.orm import sessionmaker


APP_NAME = 'bedup'

BUFSIZE = 8192

# 32MiB, initial scan takes about 12', might gain 15837689948,
# sqlite takes 256k
SIZE_CUTOFF = 32 * 1024 ** 2
# about 12' again, might gain 25807974687
SIZE_CUTOFF = 16 * 1024 ** 2
# 13'40" (36' with a backup job running in parallel), might gain 26929240347,
# sqlite takes 758k
SIZE_CUTOFF = 8 * 1024 ** 2


def get_fs(sess, volume_fd):
    fs, fs_created = get_or_create(
        sess, Filesystem,
        uuid=get_fsid(volume_fd).bytes,
        root_id=get_root_id(volume_fd))
    if fs_created:
        fs.last_tracked_generation = 0
    return fs


def track_updated_files(sess, fs, volume_fd, results_file):
    from .btrfs import ffi, u64_max

    min_generation = fs.last_tracked_generation
    top_generation = get_root_generation(volume_fd)
    results_file.write(
        'generations %d %d\n' % (min_generation, top_generation))

    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key
    lib = ffi.verifier.load_library()

    # Not a valid objectid that I know.
    # But find-new uses that and it seems to work.
    sk.tree_id = 0

    # Because we don't have min_objectid = max_objectid,
    # a min_type filter would be ineffective.
    # min_ criteria are modified by the kernel during tree traversal;
    # they are used as an iterator on tuple order,
    # not an intersection of min ranges.
    sk.min_transid = min_generation

    sk.max_objectid = u64_max
    sk.max_offset = u64_max
    sk.max_transid = u64_max
    sk.max_type = lib.BTRFS_INODE_ITEM_KEY

    while True:
        sk.nr_items = 4096

        try:
            fcntl.ioctl(
                volume_fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
        except IOError:
            raise

        if sk.nr_items == 0:
            break

        offset = 0
        for item_id in xrange(sk.nr_items):
            sh = ffi.cast(
                'struct btrfs_ioctl_search_header *', args.buf + offset)
            offset += ffi.sizeof('struct btrfs_ioctl_search_header') + sh.len

            # We can't prevent the search from grabbing irrelevant types
            if sh.type == lib.BTRFS_INODE_ITEM_KEY:
                item = ffi.cast(
                    'struct btrfs_inode_item *', sh + 1)
                found_gen = lib.btrfs_stack_inode_generation(item)
                size = lib.btrfs_stack_inode_size(item)
                mode = lib.btrfs_stack_inode_mode(item)
                if size < SIZE_CUTOFF:
                    continue
                if found_gen < min_generation:
                    continue
                if not stat.S_ISREG(mode):
                    continue
                ino = sh.objectid
                inode, created = get_or_create(
                    sess,
                    Inode,
                    fs=fs,
                    inode=ino)
                inode.size = size
                inode.has_updates = True
                names = list(lookup_ino_paths(volume_fd, ino))
                results_file.write(
                    'item type %d inode %d len %d'
                    ' gen0 %d gen1 %d size %d names %r mode %o\n' % (
                        sh.type, ino, sh.len,
                        sh.transid, found_gen, size, names,
                        mode))
        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        sk.min_offset += 1
    fs.last_tracked_generation = top_generation
    sess.commit()


def dedup(sess, fs, volume_fd, results_file):
    space_gain1 = space_gain2 = 0

    for comm1 in sess.query(
        Commonality1
    ).filter_by(
        fs_id=fs.id,
    ):
        space_gain1 += comm1.size * (len(comm1.inodes) - 1)
        results_file.write(
            'dupe candidates for size %d\n'
            % (comm1.size, ))
        for inode in comm1.inodes:
            # XXX Need to cope with deleted inodes.
            # We cannot find them in the search-new pass,
            # not without doing some tracking of directory modifications to
            # poke updated directories to find removed elements.

            # rehash everytime for now
            # I don't know enough about how inode transaction numbers
            # are updated (as opposed to extent updates)
            # to be able to actually cache the result
            try:
                paths = list(lookup_ino_paths(volume_fd, inode.inode))
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                # We have a stale record for a removed inode
                # XXX If an inode number is reused and the second instance is
                # below the size cutoff, we won't update the .size
                # attribute and we won't get an IOError to notify us either.
                # Inode reuse does happen (with and without inode_cache),
                # so this branch isn't enough to rid us of all stale entries.
                # We can also get into trouble with regular file inodes
                # being replaced by some other kind of inode.
                sess.delete(inode)
                continue
            #results_file.write('paths %r inode %d\n' % (paths, inode.inode))
            rfile = fopenat(volume_fd, paths[0])
            inode.mini_hash_from_file(rfile)

    for comm2 in sess.query(
        Commonality2
    ).filter_by(
        fs_id=fs.id,
    ):
        space_gain2 += comm2.size * (len(comm2.inodes) - 1)
        results_file.write(
            'dupe candidates for size %d and mini_hash %#x\n'
            % (comm2.size, comm2.mini_hash))
        files = []
        fds = []
        fd_names = {}
        by_hash = collections.defaultdict(list)

        for inode in comm2.inodes:
            paths = list(lookup_ino_paths(volume_fd, inode.inode))
            #results_file.write('inode %d paths %s\n' % (inode.inode, paths))
            # Open everything rw, we don't know which
            # can be a read-only source yet.
            # We may also want to defragment the source.
            try:
                afile = fopenat_rw(volume_fd, paths[0])
            except IOError as e:
                # File contains the image of a running process,
                # we can't open it in write mode.
                if e.errno == errno.ETXTBSY:
                    continue
                raise
            fd_names[afile.fileno()] = paths[0]
            files.append(afile)
            fds.append(afile.fileno())

        with ImmutableFDs(fds) as immutability:
            for afile in files:
                afd = afile.fileno()
                if afd in immutability.fds_in_write_use:
                    aname = fd_names[afd]
                    results_file.write('File %r is in use, skipping' % aname)
                    continue
                hasher = hashlib.sha1()
                for buf in iter(lambda: afile.read(BUFSIZE), ''):
                    hasher.update(buf)
                by_hash[hasher.digest()].append(afile)
            for fileset in by_hash.itervalues():
                if len(fileset) < 2:
                    continue
                sfile = fileset[0]
                sfd = sfile.fileno()
                # XXX Make this optional, defragmentation can unshare extents.
                # It can also disable compression as a side-effect.
                if False:
                    defragment(sfd)
                dfiles = fileset[1:]
                for dfile in dfiles:
                    dfd = dfile.fileno()
                    sname = fd_names[sfd]
                    dname = fd_names[dfd]
                    if not cmp_files(sfile, dfile):
                        # Probably a bug since we just used a crypto hash
                        results_file.write('Files differ: %r %r\n' % (
                            sname, dname))
                        assert False, (sname, dname)
                        continue
                    if clone_data(dest=dfd, src=sfd, check_first=True):
                        results_file.write(
                            'Did dedup: %r %r\n' % (sname, dname))
                    else:
                        results_file.write(
                            'Did not dedup (same extents): %r %r\n' % (
                                sname, dname))

    results_file.write(
        'Potential space gain: pass 1 %d, pass 2 %d\n' % (
            space_gain1, space_gain2))

    sess.execute(
        Inode.__table__.update().where(
            Inode.fs == fs
        ).values(
            has_updates=False))
    sess.commit()


def cmd_dedup_files(args):
    return dedup_same(args.source, args.dests, args.defragment)


def cmd_find_new(args):
    volume_fd = os.open(args.volume, os.O_DIRECTORY)
    # May raise FindError, let Python print it
    find_new(volume_fd, args.generation, sys.stdout)


def cmd_scan_vol(args):
    return vol_cmd(args, scan_only=True)


def cmd_dedup_vol(args):
    return vol_cmd(args, scan_only=False)


def vol_cmd(args, scan_only):
    data_dir = xdg.BaseDirectory.save_data_path(APP_NAME)
    url = sqlalchemy.engine.url.URL(
        'sqlite', database=os.path.join(data_dir, 'db.sqlite'))
    engine = sqlalchemy.engine.create_engine(url, echo=args.show_sql)
    Session = sessionmaker(bind=engine)
    sess = Session()
    META.create_all(engine)
    volume_fd = os.open(args.volume, os.O_DIRECTORY)
    fs = get_fs(sess, volume_fd)

    set_idle_priority()
    # May raise IOError, let Python print it
    track_updated_files(sess, fs, volume_fd, sys.stdout)

    if not scan_only:
        dedup(sess, fs, volume_fd, sys.stdout)


def vol_flags(parser):
    parser.add_argument('volume', help='volume to search')
    parser.add_argument(
        '--show-sql', action='store_true', dest='show_sql',
        help='print SQL statements being executed')


def main():
    parser = argparse.ArgumentParser(prog='python -m bedup')
    commands = parser.add_subparsers()

    sp_scan_vol = commands.add_parser('scan-vol')
    sp_scan_vol.set_defaults(action=cmd_scan_vol)
    vol_flags(sp_scan_vol)

    sp_dedup_vol = commands.add_parser('dedup-vol')
    sp_dedup_vol.set_defaults(action=cmd_dedup_vol)
    vol_flags(sp_dedup_vol)

    sp_dedup_files = commands.add_parser(
        'dedup-files', description="""
Freezes files, checks them for being identical,
and projects the extents of the first file onto the other files.

The effects are visible with filefrag -v (apt:e2fsprogs),
which displays the extent map of files.
        """.strip())
    sp_dedup_files.set_defaults(action=cmd_dedup_files)
    sp_dedup_files.add_argument('source', metavar='SRC', help='source file')
    sp_dedup_files.add_argument(
        'dests', metavar='DEST', nargs='+', help='dest files')
    sp_dedup_files.add_argument(
        '--defragment', action='store_true',
        help='defragment the source file first')

    sp_find_new = commands.add_parser(
        'find-new', description="""
lists changes to volume since generation

This is a reimplementation of btrfs find-new,
modified to include directories as well.""")
    sp_find_new.set_defaults(action=cmd_find_new)
    sp_find_new.add_argument('volume', help='volume to search')
    sp_find_new.add_argument(
        'generation', type=int, nargs='?', default=0,
        help='only show items modified at generation or a newer transaction')

    args = parser.parse_args()
    return args.action(args)


if __name__ == '__main__':
    sys.exit(main())

