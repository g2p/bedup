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
import os
import re
import subprocess
import sys

from collections import namedtuple, defaultdict, OrderedDict
from itertools import chain
from uuid import UUID

from .btrfs import (
    get_fsid, get_root_id,
    read_root_tree, BTRFS_FIRST_FREE_OBJECTID)
from .model import (
    BtrfsFilesystem, Volume, get_or_create, VolumePathHistory)


# 32MiB, initial scan takes about 12', might gain 15837689948,
# sqlite takes 256k
DEFAULT_SIZE_CUTOFF = 32 * 1024 ** 2
# about 12' again, might gain 25807974687
DEFAULT_SIZE_CUTOFF = 16 * 1024 ** 2
# 13'40" (36' with a backup job running in parallel), might gain 26929240347,
# sqlite takes 758k
DEFAULT_SIZE_CUTOFF = 8 * 1024 ** 2


DeviceInfo = namedtuple('DeviceInfo', 'label devices')
MountInfo = namedtuple('MountInfo', 'internal_path mpoint readonly')


class WholeFS(object):
    """A singleton representing the local filesystem"""

    def __init__(self, sess):
        # Public functions that rely on sess:
        # get_vol, get_fs, iter_fs, load_all_writable_vols, load_vols,
        # search_available_paths (get_vol doesn't have external users atm)
        # Requiring root:
        # get_vol, load_all_writable_vols, load_vols.
        # search_available_paths needs root to be useful.
        self.sess = sess
        self._mpoints_by_dev = None
        self._device_info = None

    def get_fs(self, uuid):
        fs, fs_created = get_or_create(self.sess, BtrfsFilesystem, uuid=uuid)
        if not hasattr(fs, 'root_info'):
            fs.root_info = None
            fs.mpoints = None
            fs.available_paths = None
            fs.closest_minfo = {}
        else:
            assert hasattr(fs, 'mpoints')
        if uuid in self.device_info:
            fs.label = self.device_info[uuid].label
        return fs

    def iter_fs(self):
        seen_fs_ids = []
        for (uuid, di) in self.device_info.iteritems():
            fs = self.get_fs(uuid)
            seen_fs_ids.append(fs.id)
            yield fs, di

        for uuid in self.sess.query(
            BtrfsFilesystem.uuid
        ).filter(
            ~ BtrfsFilesystem.id.in_(seen_fs_ids)
        ):
            uuid, = uuid
            yield self.get_fs(uuid), None

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
            self._ensure_root_info_read(fs, volume_fd)
            vol.root_info = fs.root_info[vol.root_id]
        return vol

    def _close_vol(self, vol):
        os.close(vol.fd)
        del vol.fd

    def _closest_minfo(self, fs, volpath):
        if volpath not in fs.closest_minfo:
            self._ensure_mount_info(fs)
            rp = os.path.realpath(volpath)

            minfos = chain(*fs.mpoints.itervalues())
            # I'm not sure mountinfo enforces realpath?
            fs.closest_minfo[volpath] = max(
                (mi for mi in minfos
                     if rp == mi.mpoint or rp.startswith(mi.mpoint + '/')),
                key=lambda mi: len(mi))
        return fs.closest_minfo[volpath]

    @property
    def mpoints_by_dev(self):
        if self._mpoints_by_dev is None:
            mbd = defaultdict(list)
            with open('/proc/self/mountinfo') as mounts:
                for line in mounts:
                    items = line.split()
                    idx = items.index('-')
                    fs_type = items[idx + 1]
                    opts1 = items[5].split(',')
                    opts2 = items[idx + 3].split(',')
                    readonly = 'ro' in opts1 + opts2
                    if fs_type != 'btrfs':
                        continue
                    intpath = items[3]
                    mpoint = items[4]
                    dev = os.path.realpath(items[idx + 2])
                    mbd[dev].append(MountInfo(intpath, mpoint, readonly))
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
                uuid = uuid.decode('ascii')
                if uuid in self._device_info:
                    # btrfs raid
                    assert self._device_info[uuid].label == label
                    self._device_info[uuid].devices.append(dev)
                else:
                    self._device_info[uuid] = DeviceInfo(label, [dev])
        return self._device_info

    def load_all_writable_vols(self, tt, size_cutoff):
        # All visible, non-ro, non-frozen volumes
        # XXX Same failure mode as load_vols
        loaded = []
        for (uuid, di) in self.device_info.iteritems():
            fs = self.get_fs(uuid)
            self._ensure_root_info(fs)
            if not fs.mpoints:
                continue
            mpoints = []
            for minfos in fs.mpoints.itervalues():
                for mi in minfos:
                    mpoints.append(mi.mpoint)
            lo, sta = self._load_visible_vols(fs, mpoints, size_cutoff)
            frozen_skipped = ro_skipped = 0
            for vol in lo:
                mi = self._closest_minfo(fs, vol.desc)
                if vol.root_info.is_frozen:
                    self._close_vol(vol)
                    frozen_skipped += 1
                elif mi.readonly:
                    self._close_vol(vol)
                    ro_skipped += 1
                else:
                    loaded.append(vol)
            if frozen_skipped:
                tt.notify(
                    'Skipped %d frozen volumes in filesystem %s %s' % (
                        frozen_skipped, fs.label, fs.uuid))
            if ro_skipped:
                tt.notify(
                    'Skipped %d read-only volumes in filesystem %s %s' % (
                        ro_skipped, fs.label, fs.uuid))
        return loaded

    def load_vols(self, volpaths, tt, size_cutoff, recurse):
        # The volume at volpath, plus all its visible non-frozen descendants
        # XXX Some of these may fail if other filesystems
        # are mounted on top of them.
        loaded = OrderedDict()
        for volpath in volpaths:
            vol = self.get_vol(volpath, size_cutoff)
            if recurse:
                self._ensure_root_info(vol.fs)
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

    def _ensure_root_info(self, fs):
        if fs.root_info is not None:
            return
        self._read_mounts_and_roots(fs, need_root_info=True)

    def _ensure_mount_info(self, fs):
        if fs.mpoints is not None:
            return
        self._read_mounts_and_roots(fs, need_root_info=False)

    def _read_mounts_and_roots(self, fs, need_root_info):
        mpoints = defaultdict(list)
        for dev in self.device_info[fs.uuid].devices:
            self._read_mounts_and_roots1(
                fs, dev, mpoints, need_root_info=need_root_info)
        fs.mpoints = dict(mpoints)

    def _ensure_root_info_read(self, fs, vol_fd):
        if fs.root_info is None:
            fs.root_info = read_root_tree(vol_fd)

    def search_available_paths(self, fs):
        if fs.available_paths is not None:
            return

        self._ensure_mount_info(fs)
        # soft-fail with no root info
        if not fs.root_info:
            return

        ap = defaultdict(set)
        for (root_id, ri, top_level) in self._iter_subvols(fs, fs.mpoints):
            if top_level:
                start_mpoints = fs.mpoints[root_id]
                ap[root_id].update(mi.mpoint for mi in start_mpoints)
                start_intpath = ri.path
                # relpath is more predictable with absolute paths;
                # otherwise it relies on getcwd (via abspath)
                assert os.path.isabs(start_intpath)
            else:
                relpath = os.path.relpath(ri.path, start_intpath)
                ap[root_id].update(
                    os.path.join(mi.mpoint, relpath) for mi in start_mpoints)
        fs.available_paths = dict(ap)

    def _read_mounts_and_roots1(self, fs, dev, mpoints, need_root_info):
        # Tends to be a less descriptive name, so keep the original
        # name blkid gave for printing.
        dev_canonical = os.path.realpath(dev)

        if dev_canonical not in self.mpoints_by_dev:
            # Known to blkid, but not mounted, or in case of raid,
            # not mounted from this device.
            # TODO: peek with a private mount?
            # Only if it can be completely safe and read-only.
            return

        for minfo in self.mpoints_by_dev[dev_canonical]:
            mpoint_fd = os.open(minfo.mpoint, os.O_DIRECTORY)
            try:
                if not is_subvolume(mpoint_fd):
                    continue
                try:
                    root_id = get_root_id(mpoint_fd)
                except IOError as e:
                    if e.errno == errno.EPERM and not need_root_info:
                        # Unlikely to work on the next loop iteration,
                        # but try anyway.
                        continue
                    raise
                self._ensure_root_info_read(fs, mpoint_fd)
            finally:
                os.close(mpoint_fd)
            assert fs.root_info[root_id].path == minfo.internal_path
            mpoints[root_id].append(minfo)


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
            whole_fs.search_available_paths(fs)
            show_fs(fs, print_indented)
        elif device_filter is None:
            sys.stdout.write(
                'UUID: %s\n  <no device available>\n' % (fs.uuid,))
            show_fs(fs, print_indented)

    if not found:
        sys.stderr.write(
            'Filesystem at %s was not found\n' % fsuuid_or_device)
    whole_fs.sess.commit()

