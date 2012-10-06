# vim: set fileencoding=utf-8 sw=4 ts=4 et :
import collections
import errno
import fcntl
import hashlib
import os
import stat

from .btrfs import (
    lookup_ino_paths, get_fsid, get_root_id,
    get_root_generation, clone_data, defragment)
from .dedup import ImmutableFDs, cmp_files
from .openat import fopenat, fopenat_rw
from .model import (
    Filesystem, Volume, Inode, comm_mappings, get_or_create,
    DedupEvent, DedupEventInode)


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


def track_updated_files(sess, vol, results_file, verbose_scan):
    from .btrfs import ffi, u64_max

    top_generation = get_root_generation(vol.fd)
    if (vol.last_tracked_size_cutoff is not None
        and vol.last_tracked_size_cutoff <= vol.size_cutoff):
        min_generation = vol.last_tracked_generation
    else:
        min_generation = 0
    results_file.write(
        'Scanning generations from %d to %d, with size cutoff %d\n'
        % (min_generation, top_generation, vol.size_cutoff))

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
                found_gen = lib.btrfs_stack_inode_generation(item)
                size = lib.btrfs_stack_inode_size(item)
                mode = lib.btrfs_stack_inode_mode(item)
                if size < vol.size_cutoff:
                    continue
                if size >= vol.last_tracked_size_cutoff:
                    if found_gen <= vol.last_tracked_generation:
                        continue
                else:
                    if found_gen <= min_generation:
                        continue
                if not stat.S_ISREG(mode):
                    continue
                ino = sh.objectid
                inode, created = get_or_create(
                    sess, Inode, vol=vol, ino=ino)
                inode.size = size
                inode.has_updates = True
                if verbose_scan:
                    names = list(lookup_ino_paths(vol.fd, ino))
                    results_file.write(
                        'item type %d ino %d len %d'
                        ' gen0 %d gen1 %d size %d names %r mode %o\n' % (
                            sh.type, ino, sh.len,
                            sh.transid, found_gen, size, names,
                            mode))
        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        sk.min_offset += 1
    vol.last_tracked_generation = top_generation
    vol.last_tracked_size_cutoff = vol.size_cutoff
    sess.commit()


def dedup_tracked(sess, volset, results_file):
    space_gain1 = space_gain2 = space_gain3 = 0
    vol_ids = [vol.id for vol in volset]
    fs = vol.fs
    assert all(vol.fs == fs for vol in volset)
    Commonality1, Commonality2, Commonality3 = comm_mappings(vol_ids)

    for comm1 in sess.query(
        Commonality1
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
            #results_file.write('paths %r ino %d\n' % (paths, inode.ino))
            rfile = fopenat(inode.vol.fd, paths[0])
            inode.mini_hash_from_file(rfile)

    for comm2 in sess.query(
        Commonality2
    ):
        space_gain2 += comm2.size * (len(comm2.inodes) - 1)
        results_file.write(
            'dupe candidates for size %d\n'
            % (comm2.size, ))
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

    for comm3 in sess.query(
        Commonality3
    ):
        space_gain3 += comm3.size * (len(comm3.inodes) - 1)
        results_file.write(
            'dupe candidates for size %d and mini_hash %#x\n'
            % (comm3.size, comm3.mini_hash))
        files = []
        fds = []
        fd_names = {}
        fd_inodes = {}
        by_hash = collections.defaultdict(list)

        for inode in comm3.inodes:
            paths = list(lookup_ino_paths(inode.vol.fd, inode.ino))
            #results_file.write('ino %d paths %s\n' % (inode.ino, paths))
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
                    results_file.write('File %r is in use, skipping' % aname)
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
                        results_file.write('Files differ: %r %r\n' % (
                            sname, dname))
                        assert False, (sname, dname)
                        continue
                    if clone_data(dest=dfd, src=sfd, check_first=True):
                        results_file.write(
                            'Did dedup: %r %r\n' % (sname, dname))
                        dfiles_successful.append(dfile)
                    else:
                        results_file.write(
                            'Did not dedup (same extents): %r %r\n' % (
                                sname, dname))
                if dfiles_successful:
                    evt = DedupEvent(fs=fs, item_size=comm3.size)
                    sess.add(evt)
                    for dfile in dfiles_successful:
                        inode = fd_inodes[dfile.fileno()]
                        evti = DedupEventInode(
                            event=evt, ino=inode.ino, vol=inode.vol)
                        sess.add(evti)
                    sess.commit()

    results_file.write(
        'Potential space gain: pass 1 %d, pass 2 %d pass 3 %d\n' % (
            space_gain1, space_gain2, space_gain3))

    sess.execute(
        Inode.__table__.update().where(
            Inode.vol_id.in_(vol_ids)
        ).values(
            has_updates=False))
    sess.commit()

