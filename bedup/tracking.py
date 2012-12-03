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

import errno
import fcntl
import gc
import hashlib
import os
import re
import resource
import stat
import subprocess
import sys

from collections import namedtuple, defaultdict, OrderedDict
from contextlib import closing
from contextlib2 import ExitStack
from sqlalchemy import and_
from uuid import UUID

from .btrfs import (
    lookup_ino_path_one, get_fsid, get_root_id,
    get_root_generation, clone_data, defragment,
    read_root_tree, BTRFS_FIRST_FREE_OBJECTID)
from .datetime import system_now
from .dedup import ImmutableFDs, cmp_files
from .openat import fopenat, fopenat_rw
from .model import (
    Filesystem, Volume, Inode, comm_mappings, get_or_create,
    DedupEvent, DedupEventInode, VolumePathHistory)


BUFSIZE = 8192

WINDOW_SIZE = 200

FS_ENCODING = sys.getfilesystemencoding()

# 32MiB, initial scan takes about 12', might gain 15837689948,
# sqlite takes 256k
DEFAULT_SIZE_CUTOFF = 32 * 1024 ** 2
# about 12' again, might gain 25807974687
DEFAULT_SIZE_CUTOFF = 16 * 1024 ** 2
# 13'40" (36' with a backup job running in parallel), might gain 26929240347,
# sqlite takes 758k
DEFAULT_SIZE_CUTOFF = 8 * 1024 ** 2


DeviceInfo = namedtuple('DeviceInfo', 'label devices')


class WholeFS(object):
    """A singleton representing the local filesystem"""

    def __init__(self, sess):
        self.sess = sess
        self._mpoints_by_dev = None
        self._device_info = None

    def get_fs(self, uuid):
        fs, fs_created = get_or_create(self.sess, Filesystem, uuid=uuid)
        if not hasattr(fs, 'root_info'):
            fs.root_info = None
            fs.mpoints = None
            fs.available_paths = None
        else:
            assert hasattr(fs, 'mpoints')
        if uuid in self.device_info:
            fs.label = self.device_info[uuid].label
        return fs

    def _iter_other_fs(self, excluded_fs_ids):
        query = self.sess.query(Filesystem)
        if excluded_fs_ids:
            query = query.filter(~ Filesystem.id.in_(excluded_fs_ids))
        for fs in query:
            if not hasattr(fs, 'root_info'):
                fs.root_info = None
                fs.mpoints = None
            yield fs

    def iter_fs(self):
        seen_fs_ids = []
        for (uuid, di) in self.device_info.iteritems():
            fs = self.get_fs(uuid)
            seen_fs_ids.append(fs.id)
            yield fs, di

        for fs in self._iter_other_fs(seen_fs_ids):
            yield fs, None

    def _ensure_root_info(self, fs, vol_fd):
        if fs.root_info is None:
            fs.root_info = read_root_tree(vol_fd)

    def get_vol(self, volpath, size_cutoff):
        volpath = os.path.normpath(volpath)
        volume_fd = os.open(volpath, os.O_DIRECTORY)
        fs = self.get_fs(uuid=str(get_fsid(volume_fd)))
        assert hasattr(fs, 'mpoints')
        vol, vol_created = get_or_create(
            self.sess, Volume, fs=fs, root_id=get_root_id(volume_fd))
        assert hasattr(vol.fs, 'mpoints')

        if size_cutoff is not None:
            vol.size_cutoff = size_cutoff
        elif vol_created:
            vol.size_cutoff = DEFAULT_SIZE_CUTOFF

        path_history, ph_created = get_or_create(
            self.sess, VolumePathHistory, vol=vol, path=volpath)

        # If a volume was given multiple times on the command line,
        # keep the first name and fd for it.
        if hasattr(vol, 'fd'):
            os.close(volume_fd)
        else:
            vol.fd = volume_fd
            vol.st_dev = os.fstat(volume_fd).st_dev
            # Only use the path as a description, it is liable to change.
            vol.desc = volpath
            self._ensure_root_info(fs, volume_fd)
            vol.root_info = fs.root_info[vol.root_id]
        return vol

    def _close_vol(self, vol):
        os.close(vol.fd)
        del vol.fd

    @property
    def mpoints_by_dev(self):
        if self._mpoints_by_dev is None:
            mbd = defaultdict(list)
            with open('/proc/self/mountinfo') as mounts:
                for line in mounts:
                    items = line.split()
                    idx = items.index('-')
                    fs_type = items[idx + 1]
                    if fs_type != 'btrfs':
                        continue
                    volpath = items[3]
                    mpoint = items[4]
                    dev = os.path.realpath(items[idx + 2])
                    mbd[dev].append((volpath, mpoint))
            self._mpoints_by_dev = dict(mbd)
        return self._mpoints_by_dev

    @property
    def device_info(self):
        if self._device_info is None:
            self._device_info = {}
            for line in subprocess.check_output(
                'blkid -s LABEL -s UUID -t TYPE=btrfs'.split()
            ).splitlines():
                dev, label, uuid = BLKID_RE.match(line).groups()
                if uuid in self._device_info:
                    # btrfs raid
                    assert self._device_info[uuid].label == label
                    self._device_info[uuid].devices.append(dev)
                else:
                    self._device_info[uuid] = DeviceInfo(label, [dev])
        return self._device_info

    def load_all_visible_vols(self, tt, size_cutoff):
        # All visible non-frozen volumes
        # XXX Same failure mode as load_vols
        loaded = []
        for (uuid, di) in self.device_info.iteritems():
            fs = self.get_fs(uuid)
            self.ensure_mount_info(fs)
            if fs.mpoints:
                mpoints = reduce(lambda l1, l2: l1 + l2, fs.mpoints.values())
                lo, sta = self._load_visible_vols(fs, mpoints, size_cutoff)
                skipped = 0
                for vol in lo:
                    if vol.root_info.is_frozen:
                        self._close_vol(vol)
                        skipped += 1
                    else:
                        loaded.append(vol)
                if skipped:
                    tt.notify(
                        'Skipped %d frozen volumes in filesystem %s %s' % (
                            skipped, fs.label, fs.uuid))
        return loaded

    def load_vols(self, volpaths, tt, size_cutoff, recurse):
        # The volume at volpath, plus all its visible non-frozen descendants
        # XXX Some of these may fail if other filesystems
        # are mounted on top of them.
        loaded = OrderedDict()
        for volpath in volpaths:
            vol = self.get_vol(volpath, size_cutoff)
            if recurse:
                self.ensure_mount_info(vol.fs)
                lo, sta = self._load_visible_vols(
                    vol.fs, [volpath], size_cutoff)
                skipped = 0
                for vol in lo:
                    if vol in loaded:
                        continue
                    if vol.root_info.is_frozen and vol not in sta:
                        self._close_vol(vol)
                        skipped += 1
                    else:
                        loaded[vol] = True
                if skipped:
                    tt.notify(
                        'Skipped %d frozen volumes in filesystem %s %s' % (
                            skipped, vol.fs.label, vol.fs.uuid))
            else:
                if vol not in loaded:
                    loaded[vol] = True
        return loaded.keys()

    def _iter_subvols(self, fs, start_root_ids):
        child_id_map = defaultdict(list)

        for root_id, ri in fs.root_info.iteritems():
            if ri.parent_root_id is not None:
                child_id_map[ri.parent_root_id].append(root_id)

        def _iter_children(root_id, top_level):
            yield (root_id, fs.root_info[root_id], top_level)
            for child_id in child_id_map[root_id]:
                for item in _iter_children(child_id, False):
                    yield item

        for root_id in start_root_ids:
            for item in _iter_children(root_id, True):
                yield item


    def _load_visible_vols(self, fs, start_paths, size_cutoff):
        # Use dicts, there may be repetitions under multiple mpoints
        loaded = OrderedDict()

        start_vols = OrderedDict(
            (vol.root_id, vol)
            for vol in (
                self.get_vol(start_fspath, size_cutoff)
                for start_fspath in start_paths))

        for (root_id, ri, top_level) in self._iter_subvols(fs, start_vols):
            if top_level:
                start_vol = start_vols[root_id]
                if start_vol not in loaded:
                    loaded[start_vol] = True
                start_fspath = start_vol.desc
                start_intpath = ri.path
                # relpath is more predictable with absolute paths;
                # otherwise it relies on getcwd (via abspath)
                assert os.path.isabs(start_intpath)
            else:
                relpath = os.path.relpath(ri.path, start_intpath)
                # XXX start_vol.fd would be more reliable here;
                # probably requires Python 3.3 for relative open.
                vol = self.get_vol(
                    os.path.join(start_fspath, relpath), size_cutoff)
                if vol not in loaded:
                    loaded[vol] = True
        return loaded.keys(), start_vols.values()

    def ensure_mount_info(self, fs):
        if fs.mpoints is not None:
            return

        mpoints = defaultdict(list)
        for dev in self.device_info[fs.uuid].devices:
            self._read_mount_info(fs, dev, mpoints)
        fs.mpoints = dict(mpoints)

    def ensure_available_paths(self, fs):
        if fs.available_paths is not None:
            return
        self.ensure_mount_info(fs)
        if not fs.root_info:
            return
        ap = defaultdict(set)
        for (root_id, ri, top_level) in self._iter_subvols(fs, fs.mpoints):
            if top_level:
                start_mpoints = fs.mpoints[root_id]
                ap[root_id].update(start_mpoints)
                start_intpath = ri.path
                # relpath is more predictable with absolute paths;
                # otherwise it relies on getcwd (via abspath)
                assert os.path.isabs(start_intpath)
            else:
                relpath = os.path.relpath(ri.path, start_intpath)
                ap[root_id].update(
                    os.path.join(smp, relpath) for smp in start_mpoints)
        fs.available_paths = dict(ap)

    def _read_mount_info(self, fs, dev, mpoints):
        # Tends to be a less descriptive name, so keep the original
        # name blkid gave for printing.
        dev_canonical = os.path.realpath(dev)

        if dev_canonical not in self.mpoints_by_dev:
            # Known to blkid, but not mounted, or in case of raid,
            # not mounted from this device.
            # TODO: peek with a private mount?
            # Only if it can be completely safe and read-only.
            return

        for volpath, mpoint in self.mpoints_by_dev[dev_canonical]:
            mpoint_fd = os.open(mpoint, os.O_DIRECTORY)
            try:
                if not is_subvolume(mpoint_fd):
                    continue
                try:
                    root_id = get_root_id(mpoint_fd)
                except IOError as e:
                    if e.errno == errno.EPERM:
                        # Unlikely to work on the next loop iteration,
                        # but try anyway.
                        continue
                    raise
                self._ensure_root_info(fs, mpoint_fd)
            finally:
                os.close(mpoint_fd)
            assert fs.root_info[root_id].path == volpath
            mpoints[root_id].append(mpoint)


def reset_vol(sess, vol):
    # Forgets Inodes, not logging. Make that configurable?
    sess.query(Inode).filter_by(vol=vol).delete()
    vol.last_tracked_generation = 0
    sess.commit()


BLKID_RE = re.compile(
    br'^(?P<dev>/dev/[^:]*): '
    br'(?:LABEL="(?P<label>[^"]*)" )?UUID="(?P<uuid>[^"]*)"\s*$')


def is_subvolume(btrfs_mountpoint_fd):
    st = os.fstat(btrfs_mountpoint_fd)
    return st.st_ino == BTRFS_FIRST_FREE_OBJECTID


def show_fs(fs, print_indented):
    vols_by_id = dict((vol.root_id, vol) for vol in fs.volumes)
    if fs.root_info:
        root_ids = set(fs.root_info.keys() + vols_by_id.keys())
    else:
        root_ids = vols_by_id.iterkeys()
    for root_id in sorted(root_ids):
        flags = ''
        if fs.root_info:
            if root_id not in fs.root_info:
                # The filesystem is available (we could scan the root tree),
                # so the volume must have been destroyed.
                flags = ' (deleted)'
            elif fs.root_info[root_id].is_frozen:
                flags = ' (frozen)'

        print_indented('Volume %d%s' % (root_id, flags), 0)
        try:
            vol = vols_by_id[root_id]
        except KeyError:
            pass
        else:
            if vol.inode_count:
                print_indented(
                    'As of generation %d, '
                    'tracking %d inodes of size at least %d'
                    % (vol.last_tracked_generation, vol.inode_count,
                       vol.size_cutoff), 1)

        if fs.root_info and root_id in fs.root_info:
            ri = fs.root_info[root_id]
            if root_id in fs.available_paths:
                for apath in fs.available_paths[root_id]:
                    print_indented('Accessible at %s' % apath, 1)
            else:
                print_indented('Internal path %s' % ri.path, 1)
        else:
            # We can use vol, since keys come from one or the other
            print_indented(
                'Last seen at %s' % vol.last_known_mountpoint, 1)


def show_vols(whole_fs, fsuuid_or_device):
    initial_indent = indent = '  '
    uuid_filter = device_filter = None
    found = True

    if fsuuid_or_device is not None:
        found = False
        if fsuuid_or_device[0] == '/':
            device_filter = fsuuid_or_device
            # TODO: use stat, if it's a dir,
            # call show_vol extracted from show_fs
        else:
            uuid_filter = UUID(hex=fsuuid_or_device)

    def print_indented(line, depth):
        sys.stdout.write(initial_indent + depth * indent + line + '\n')

    # Without root, we are mostly limited to what's stored in the db.
    # Can't link volume ids to mountpoints, can't list subvolumes.
    # There's just blkid sharing blkid.tab, and the kernel with mountinfo.
    # Print a warning?
    for (fs, di) in whole_fs.iter_fs():
        if uuid_filter:
            if UUID(hex=fs.uuid) == uuid_filter:
                found = True
            else:
                continue
        if di is not None:
            if device_filter:
                if device_filter in di.devices:
                    found = True
                else:
                    continue
            sys.stdout.write('Label: %s UUID: %s\n' % (di.label, fs.uuid))
            for dev in di.devices:
                print_indented('Device: %s' % (dev, ), 0)
            whole_fs.ensure_available_paths(fs)
            show_fs(fs, print_indented)
        elif device_filter is None:
            sys.stdout.write(
                'UUID: %s\n  <no device available>\n' % (fs.uuid,))
            show_fs(fs, print_indented)

    if not found:
        sys.stderr.write(
            'Filesystem at %s was not found\n' % fsuuid_or_device)
    whole_fs.sess.commit()


def fake_updates(sess, max_events):
    faked = 0
    for de in sess.query(DedupEvent).limit(max_events):
        ino_count = 0
        for dei in de.inodes:
            inode = sess.query(Inode).filter_by(ino=dei.ino, vol=dei.vol).scalar()
            if not inode:
                continue
            inode.has_updates = True
            ino_count += 1
        if ino_count > 1:
            faked += 1
    return faked


def track_updated_files(sess, vol, tt):
    from .btrfs import ffi, u64_max

    top_generation = get_root_generation(vol.fd)
    if (vol.last_tracked_size_cutoff is not None
        and vol.last_tracked_size_cutoff <= vol.size_cutoff):
        min_generation = vol.last_tracked_generation + 1
    else:
        min_generation = 0
    if min_generation > top_generation:
        tt.notify(
            'Skipping scan of %r, generation is still %d'
            % (vol.desc, top_generation))
        sess.commit()
        return
    tt.notify(
        'Scanning volume %r generations from %d to %d, with size cutoff %d'
        % (vol.desc, min_generation, top_generation, vol.size_cutoff))
    tt.format(
        '{elapsed} Updated {desc:counter} items: '
        '{path:truncate-left} {desc}')

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
                if (vol.last_tracked_size_cutoff
                    and size >= vol.last_tracked_size_cutoff):
                    if inode_gen <= vol.last_tracked_generation:
                        continue
                else:
                    if inode_gen < min_generation:
                        continue
                if not stat.S_ISREG(mode):
                    continue
                ino = sh.objectid
                inode, inode_created = get_or_create(
                    sess, Inode, vol=vol, ino=ino)
                inode.size = size
                inode.has_updates = True

                try:
                    path = lookup_ino_path_one(vol.fd, ino)
                except IOError as e:
                    tt.notify(
                        'Error at path lookup of inode %d: %r' % (ino, e))
                    if inode_created:
                        sess.expunge(inode)
                    else:
                        sess.delete(inode)
                    continue

                try:
                    path = path.decode(FS_ENCODING)
                except ValueError:
                    continue
                tt.update(path=path)
                tt.update(
                    desc='(ino %d outer gen %d inner gen %d size %d)' % (
                        ino, sh.transid, inode_gen, size))
        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        sk.min_offset += 1
    tt.format(None)
    vol.last_tracked_generation = top_generation
    vol.last_tracked_size_cutoff = vol.size_cutoff
    sess.commit()


def windowed_query(window_start, query, attr, per, clear_updates):
    # [window_start, window_end] is inclusive at both ends
    # Figure out how to use attr for property access as well?
    query = query.order_by(-attr)

    while True:
        li = query.filter(attr <= window_start).limit(per).all()
        if not li:
            clear_updates(window_start, 0)
            return
        for el in li:
            yield el
        window_end = el.size
        clear_updates(window_start, window_end)
        window_start = window_end - 1


def dedup_tracked(sess, volset, tt):
    skipped = []
    fs = volset[0].fs
    vol_ids = [vol.id for vol in volset]
    assert all(vol.fs == fs for vol in volset)

    # 3 for stdio, 3 for sqlite (wal mode), 1 that somehow doesn't
    # get closed, 1 per volume.
    ofile_reserved = 7 + len(volset)

    FilteredInode, Commonality1, unregister = comm_mappings(fs.id, vol_ids)
    query = sess.query(Commonality1)
    le = query.count()

    def clear_updates(window_start, window_end):
        # Can't call update directly on FilteredInode because it is aliased.
        sess.execute(
            Inode.__table__.update().where(and_(
                Inode.vol_id.in_(vol_ids),
                window_start >= Inode.size >= window_end
            )).values(
                has_updates=False))

        for inode in skipped:
            inode.has_updates = True
        sess.commit()
        # clear the list
        skipped[:] = []

    if le:
        tt.format('{elapsed} Size group {comm1:counter}/{comm1:total}')
        tt.set_total(comm1=le)

        # This is higher than query.first().size, and will also clear updates
        # without commonality.
        window_start = sess.query(Inode).order_by(-Inode.size).first().size

        query = windowed_query(
            window_start, query, attr=Commonality1.size, per=WINDOW_SIZE,
            clear_updates=clear_updates)
        dedup_tracked1(sess, tt, ofile_reserved, query, fs, skipped)

    sess.commit()
    unregister()


def dedup_tracked1(sess, tt, ofile_reserved, query, fs, skipped):
    space_gain1 = space_gain2 = space_gain3 = 0
    ofile_soft, ofile_hard = resource.getrlimit(resource.RLIMIT_OFILE)

    # Hopefully close any files we left around
    gc.collect()

    # The log can cause frequent commits, we don't mind losing them in
    # a crash (no need for durability). SQLite is in WAL mode, so this pragma
    # should disable most commit-time fsync calls without compromising
    # consistency.
    sess.execute('PRAGMA synchronous=NORMAL;')

    for comm1 in query:
        if len(sess.identity_map) > 300:
            sess.flush()

        space_gain1 += comm1.size * (comm1.inode_count - 1)
        tt.update(comm1=comm1)
        for inode in comm1.inodes:
            # XXX Need to cope with deleted inodes.
            # We cannot find them in the search-new pass, not without doing
            # some tracking of directory modifications to poke updated
            # directories to find removed elements.

            # rehash everytime for now
            # I don't know enough about how inode transaction numbers are
            # updated (as opposed to extent updates) to be able to actually
            # cache the result
            try:
                path = lookup_ino_path_one(inode.vol.fd, inode.ino)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                # We have a stale record for a removed inode
                # XXX If an inode number is reused and the second instance
                # is below the size cutoff, we won't update the .size
                # attribute and we won't get an IOError to notify us
                # either.  Inode reuse does happen (with and without
                # inode_cache), so this branch isn't enough to rid us of
                # all stale entries.  We can also get into trouble with
                # regular file inodes being replaced by some other kind of
                # inode.
                sess.delete(inode)
                continue
            with closing(fopenat(inode.vol.fd, path)) as rfile:
                inode.mini_hash_from_file(rfile)

        for comm2 in comm1.comm2:
            space_gain2 += comm2.size * (comm2.inode_count - 1)
            tt.update(comm2=comm2)
            for inode in comm2.inodes:
                try:
                    path = lookup_ino_path_one(inode.vol.fd, inode.ino)
                except IOError as e:
                    if e.errno != errno.ENOENT:
                        raise
                    sess.delete(inode)
                    continue
                with closing(fopenat(inode.vol.fd, path)) as rfile:
                    inode.fiemap_hash_from_file(rfile)

            if not comm2.comm3:
                continue

            comm3, = comm2.comm3
            count3 = comm3.inode_count
            space_gain3 += comm3.size * (count3 - 1)
            tt.update(comm3=comm3)
            files = []
            fds = []
            fd_names = {}
            fd_inodes = {}
            by_hash = defaultdict(list)

            # XXX I have no justification for doubling count3
            ofile_req = 2 * count3 + ofile_reserved
            if ofile_req > ofile_soft:
                if ofile_req <= ofile_hard:
                    resource.setrlimit(
                        resource.RLIMIT_OFILE, (ofile_req, ofile_hard))
                    ofile_soft = ofile_req
                else:
                    tt.notify(
                        'Too many duplicates (%d at size %d), '
                        'would bring us over the open files limit (%d, %d).'
                        % (count3, comm3.size, ofile_soft, ofile_hard))
                    for inode in comm3.inodes:
                        if inode.has_updates:
                            skipped.append(inode)
                    continue

            for inode in comm3.inodes:
                # Open everything rw, we can't pick one for the source side
                # yet because the crypto hash might eliminate it.
                # We may also want to defragment the source.
                try:
                    path = lookup_ino_path_one(inode.vol.fd, inode.ino)
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        sess.delete(inode)
                        continue
                    raise
                try:
                    afile = fopenat_rw(inode.vol.fd, path)
                except IOError as e:
                    if e.errno == errno.ETXTBSY:
                        # The file contains the image of a running process,
                        # we can't open it in write mode.
                        tt.notify('File %r is busy, skipping' % path)
                        skipped.append(inode)
                        continue
                    elif e.errno == errno.EACCES:
                        # Could be SELinux or immutability
                        tt.notify('Access denied on %r, skipping' % path)
                        skipped.append(inode)
                        continue
                    elif e.errno == errno.ENOENT:
                        # The file was moved or unlinked by a racing process
                        tt.notify('File %r may have moved, skipping' % path)
                        skipped.append(inode)
                        continue
                    raise

                # It's not completely guaranteed we have the right inode,
                # there may still be race conditions at this point.
                # Gets re-checked below (tell and fstat).
                fd = afile.fileno()
                fd_inodes[fd] = inode
                fd_names[fd] = path
                files.append(afile)
                fds.append(fd)

            with ExitStack() as stack:
                for afile in files:
                    stack.enter_context(closing(afile))
                # Enter this context last
                immutability = stack.enter_context(ImmutableFDs(fds))

                for afile in files:
                    fd = afile.fileno()
                    inode = fd_inodes[fd]
                    if fd in immutability.fds_in_write_use:
                        tt.notify('File %r is in use, skipping' % fd_names[fd])
                        skipped.append(inode)
                        continue
                    hasher = hashlib.sha1()
                    for buf in iter(lambda: afile.read(BUFSIZE), b''):
                        hasher.update(buf)

                    # Gets rid of a race condition
                    st = os.fstat(fd)
                    if st.st_ino != inode.ino:
                        skipped.append(inode)
                        continue
                    if st.st_dev != inode.vol.st_dev:
                        skipped.append(inode)
                        continue

                    size = afile.tell()
                    if size != comm3.size:
                        if size < inode.vol.size_cutoff:
                            # if we didn't delete this inode, it would cause
                            # spurious comm groups in all future invocations.
                            sess.delete(inode)
                        else:
                            skipped.append(inode)
                        continue

                    by_hash[hasher.digest()].append(afile)

                for fileset in by_hash.itervalues():
                    if len(fileset) < 2:
                        continue
                    sfile = fileset[0]
                    sfd = sfile.fileno()
                    # Commented out, defragmentation can unshare extents.
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
                            tt.notify('Files differ: %r %r' % (sname, dname))
                            assert False, (sname, dname)
                            continue
                        if clone_data(dest=dfd, src=sfd, check_first=True):
                            tt.notify('Deduplicated: %r %r' % (sname, dname))
                            dfiles_successful.append(dfile)
                        else:
                            tt.notify(
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

    tt.format(None)
    tt.notify(
        'Potential space gain: pass 1 %d, pass 2 %d pass 3 %d' % (
            space_gain1, space_gain2, space_gain3))
    # Restore fsync so that the final commit (in dedup_tracked)
    # will be durable.
    sess.commit()
    sess.execute('PRAGMA synchronous=FULL;')

