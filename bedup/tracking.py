# vim: set fileencoding=utf-8 sw=4 ts=4 et :
# bedup - Btrfs deduplication
# Copyright (C) 2012 Gabriel de Perthuis <g2p.code+bedup@gmail.com>
#
# This file is part of bedup.
#
# bedup is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# bedup is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with bedup.  If not, see <http://www.gnu.org/licenses/>.

import collections
import errno
import fcntl
import hashlib
import os
import re
import stat
import subprocess
import sys
import ttystatus

from .btrfs import (
    lookup_ino_paths, get_fsid, get_root_id,
    get_root_generation, clone_data, defragment,
    volumes_from_root_tree, BTRFS_FIRST_FREE_OBJECTID)
from .datetime import system_now
from .dedup import ImmutableFDs, cmp_files
from .openat import fopenat, fopenat_rw
from .model import (
    Filesystem, Volume, Inode, comm_mappings, get_or_create,
    DedupEvent, DedupEventInode, VolumePathHistory)


BUFSIZE = 8192

# 32MiB, initial scan takes about 12', might gain 15837689948,
# sqlite takes 256k
DEFAULT_SIZE_CUTOFF = 32 * 1024 ** 2
# about 12' again, might gain 25807974687
DEFAULT_SIZE_CUTOFF = 16 * 1024 ** 2
# 13'40" (36' with a backup job running in parallel), might gain 26929240347,
# sqlite takes 758k
DEFAULT_SIZE_CUTOFF = 8 * 1024 ** 2


def get_vol(sess, volpath, size_cutoff):
    volpath = os.path.normpath(volpath)
    volume_fd = os.open(volpath, os.O_DIRECTORY)
    fs, fs_created = get_or_create(
        sess, Filesystem,
        uuid=str(get_fsid(volume_fd)))
    vol, vol_created = get_or_create(
        sess, Volume,
        fs=fs, root_id=get_root_id(volume_fd))

    if size_cutoff is not None:
        vol.size_cutoff = size_cutoff
    elif vol_created:
        vol.size_cutoff = DEFAULT_SIZE_CUTOFF

    path_history, ph_created = get_or_create(
        sess, VolumePathHistory, vol=vol, path=volpath)

    # If a volume was given multiple times on the command line,
    # keep the first name and fd for it.
    if hasattr(vol, 'fd'):
        os.close(volume_fd)
    else:
        vol.fd = volume_fd
        vol.st_dev = os.fstat(volume_fd).st_dev
        # Only use the path as a description, it is liable to change.
        vol.desc = volpath
    return vol


def forget_vol(sess, vol):
    # Forgets Inodes, not logging. Make that configurable?
    sess.query(Inode).filter_by(vol=vol).delete()
    vol.last_tracked_generation = 0
    sess.commit()


BLKID_RE = re.compile(
    '^(?P<dev>/dev/[^:]*): '
    'LABEL="(?P<label>[^"]*)" UUID="(?P<uuid>[^"]*)"\s*$')


def show_vols(sess):
    mpoints_by_dev = collections.defaultdict(list)
    with open('/proc/self/mountinfo') as mounts:
        for line in mounts:
            items = line.split()
            idx = items.index('-')
            fs_type = items[idx + 1]
            if fs_type != 'btrfs':
                continue
            volpath = items[3]
            mpoint = items[4]
            dev = items[idx + 2]
            mpoints_by_dev[dev].append((volpath, mpoint))

    for line in subprocess.check_output(
        'blkid -s LABEL -s UUID -t TYPE=btrfs'.split()
    ).splitlines():
        dev, label, uuid = BLKID_RE.match(line).groups()
        sys.stdout.write('%s\n  Label: %s UUID: %s\n' % (dev, label, uuid))
        fs = sess.query(Filesystem).filter_by(uuid=uuid).scalar()
        if fs is not None:
            mpoint_by_root_id = collections.defaultdict(list)
            for (volpath, mpoint) in mpoints_by_dev[dev]:
                mpoint_fd = os.open(mpoint, os.O_DIRECTORY)
                st = os.fstat(mpoint_fd)
                if st.st_ino != BTRFS_FIRST_FREE_OBJECTID:
                    # Not the root of a subvolume
                    continue

                try:
                    mpoint_by_root_id[get_root_id(mpoint_fd)].append(
                        (volpath, mpoint))
                    if False:
                        volumes_from_root_tree(mpoint_fd)
                except IOError as e:
                    if e.errno == errno.EPERM:
                        break
                    raise

            for vol in fs.volumes:
                # Show the volume path, which requires
                # finding the volume in the tree of tree roots?
                # That may require a subvol=/ mount, existing
                # mounts may not exist or may not be at subvolume paths.
                sys.stdout.write(
                    '    Volume %d last tracked generation %d size cutoff %d\n'
                    % (vol.root_id, vol.last_tracked_generation,
                       vol.size_cutoff))

                if vol.root_id in mpoint_by_root_id:
                    for (volpath, mpoint) in mpoint_by_root_id[vol.root_id]:
                        sys.stdout.write('      Mounted on %s\n' % mpoint)
                    sys.stdout.write('      Path %s\n' % volpath)


def track_updated_files(sess, vol):
    from .btrfs import ffi, u64_max

    top_generation = get_root_generation(vol.fd)
    if (vol.last_tracked_size_cutoff is not None
        and vol.last_tracked_size_cutoff <= vol.size_cutoff):
        min_generation = vol.last_tracked_generation
    else:
        min_generation = 0
    ts = ttystatus.TerminalStatus(period=.1)
    ts.notify(
        'Scanning volume %r generations from %d to %d, with size cutoff %d'
        % (vol.desc, min_generation, top_generation, vol.size_cutoff))
    ts.format(
        '%ElapsedTime() Updated %Counter(desc) items: '
        '%Pathname(path) %String(desc)')

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
                vol.fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
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
                inode_gen = lib.btrfs_stack_inode_generation(item)
                size = lib.btrfs_stack_inode_size(item)
                mode = lib.btrfs_stack_inode_mode(item)
                if size < vol.size_cutoff:
                    continue
                # XXX Should I use inner or outer gen in these checks?
                # Inner gen seems to miss updates (due to delalloc?),
                # whereas outer gen has too many spurious updates.
                if size >= vol.last_tracked_size_cutoff:
                    if inode_gen <= vol.last_tracked_generation:
                        continue
                else:
                    if inode_gen <= min_generation:
                        continue
                if not stat.S_ISREG(mode):
                    continue
                ino = sh.objectid
                if ino in (541144, 691635, 1379998):  # Yeah...
                    continue
                inode, inode_created = get_or_create(
                    sess, Inode, vol=vol, ino=ino)
                inode.size = size
                inode.has_updates = True

                try:
                    # Fail early
                    names = list(lookup_ino_paths(vol.fd, ino))
                except IOError as e:
                    ts.notify('Error at path lookup: %r' % e)
                    if inode_created:
                        sess.expunge(inode)
                    else:
                        sess.delete(inode)
                    continue

                ts['path'] = names[0]
                ts['desc'] = (
                        '(ino %d outer gen %d inner gen %d size %d)' % (
                            ino, sh.transid, inode_gen, size))
        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        sk.min_offset += 1
    ts.finish()
    vol.last_tracked_generation = top_generation
    vol.last_tracked_size_cutoff = vol.size_cutoff
    sess.commit()


def dedup_tracked(sess, volset):
    space_gain1 = space_gain2 = space_gain3 = 0
    vol_ids = [vol.id for vol in volset]
    fs = vol.fs
    assert all(vol.fs == fs for vol in volset)

    def end():
        sess.execute(
            Inode.__table__.update().where(
                Inode.vol_id.in_(vol_ids)
            ).values(
                has_updates=False))
        sess.commit()

    Commonality1, Commonality2, Commonality3 = comm_mappings(vol_ids)

    ts = ttystatus.TerminalStatus(period=.1)
    # Make a list so we can get the length without querying twice
    # Might be wasteful if the common set is really big though.
    query = list(sess.query(Commonality1))
    le = len(query)
    if not le:
        end()
        return
    ts.format(
        '%ElapsedTime() Partial hash of same-size groups '
        '%Counter(comm1)/{le}'.format(le=le))
    for comm1 in query:
        space_gain1 += comm1.size * (len(comm1.inodes) - 1)
        ts['comm1'] = comm1
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
                paths = list(lookup_ino_paths(inode.vol.fd, inode.ino))
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
            rfile = fopenat(inode.vol.fd, paths[0])
            inode.mini_hash_from_file(rfile)
    ts.finish()
    ts.clear()

    query = list(sess.query(Commonality2))
    le = len(query)
    if not le:
        end()
        return
    ts.format(
        '%ElapsedTime() Extent map %Counter(comm2)/{le}'.format(le=le))
    for comm2 in query:
        space_gain2 += comm2.size * (len(comm2.inodes) - 1)
        ts['comm2'] = comm2
        for inode in comm2.inodes:
            try:
                paths = list(lookup_ino_paths(inode.vol.fd, inode.ino))
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                sess.delete(inode)
                continue
            rfile = fopenat(inode.vol.fd, paths[0])
            inode.fiemap_hash_from_file(rfile)
    ts.finish()
    ts.clear()

    query = list(sess.query(Commonality3))
    le = len(query)
    if not le:
        end()
        return
    ts.format(
        '%ElapsedTime() Full hash and deduplication %Counter(comm3)/{le}'
        .format(le=le))
    for comm3 in query:
        space_gain3 += comm3.size * (len(comm3.inodes) - 1)
        ts['comm3'] = comm3
        files = []
        fds = []
        fd_names = {}
        fd_inodes = {}
        by_hash = collections.defaultdict(list)

        for inode in comm3.inodes:
            paths = list(lookup_ino_paths(inode.vol.fd, inode.ino))
            # Open everything rw, we can't pick one for the source side
            # yet because the crypto hash might eliminate it.
            # We may also want to defragment the source.
            try:
                afile = fopenat_rw(inode.vol.fd, paths[0])
            except IOError as e:
                # File contains the image of a running process,
                # we can't open it in write mode.
                if e.errno == errno.ETXTBSY:
                    continue
                raise

            # It's not completely guaranteed we have the right inode,
            # there may still be race conditions at this point.
            # Gets re-checked below (tell and fstat).
            fd_inodes[afile.fileno()] = inode
            fd_names[afile.fileno()] = paths[0]
            files.append(afile)
            fds.append(afile.fileno())

        with ImmutableFDs(fds) as immutability:
            for afile in files:
                afd = afile.fileno()
                if afd in immutability.fds_in_write_use:
                    aname = fd_names[afd]
                    ts.notify('File %r is in use, skipping' % aname)
                    continue
                hasher = hashlib.sha1()
                for buf in iter(lambda: afile.read(BUFSIZE), ''):
                    hasher.update(buf)

                # Mostly for the sake of correct logging, might also
                # prevent some form of security exploitation.
                # The file will pop up in the next scan anyway.
                if afile.tell() != comm3.size:
                    continue
                st = os.fstat(afd)
                if st.st_ino != fd_inodes[afd].ino:
                    continue
                if st.st_dev != fd_inodes[afd].vol.st_dev:
                    continue

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
                dfiles_successful = []
                for dfile in dfiles:
                    dfd = dfile.fileno()
                    sname = fd_names[sfd]
                    dname = fd_names[dfd]
                    if not cmp_files(sfile, dfile):
                        # Probably a bug since we just used a crypto hash
                        ts.notify('Files differ: %r %r' % (sname, dname))
                        assert False, (sname, dname)
                        continue
                    if clone_data(dest=dfd, src=sfd, check_first=True):
                        ts.notify('Deduplicated: %r %r' % (sname, dname))
                        dfiles_successful.append(dfile)
                    else:
                        ts.notify(
                            'Did not deduplicate (same extents): %r %r' % (
                                sname, dname))
                if dfiles_successful:
                    evt = DedupEvent(
                        fs=fs, item_size=comm3.size, created=system_now())
                    sess.add(evt)
                    for afile in [sfile] + dfiles_successful:
                        inode = fd_inodes[afile.fileno()]
                        evti = DedupEventInode(
                            event=evt, ino=inode.ino, vol=inode.vol)
                        sess.add(evti)
                    sess.commit()
    ts.finish()
    ts.clear()

    ts.notify(
        'Potential space gain: pass 1 %d, pass 2 %d pass 3 %d' % (
            space_gain1, space_gain2, space_gain3))

    end()

