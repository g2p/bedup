# vim: set fileencoding=utf-8 sw=4 ts=4 et :
# bedup - Btrfs deduplication
# Copyright (C) 2015 Gabriel de Perthuis <g2p.code+bedup@gmail.com>
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
import tempfile

from collections import namedtuple, defaultdict, OrderedDict, Counter
from uuid import UUID

from sqlalchemy.util import memoized_property
from sqlalchemy.orm.exc import NoResultFound

from .platform.btrfs import (
    get_fsid, get_root_id, lookup_ino_path_one,
    read_root_tree, BTRFS_FIRST_FREE_OBJECTID)
from .platform.openat import openat
from .platform.unshare import unshare, CLONE_NEWNS

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
MountInfo = namedtuple('MountInfo', 'internal_path mpoint readonly private')

# A description, which may or may not be a path in the global filesystem
VolDesc = namedtuple('VolDesc', 'description is_fs_path')


class NotMounted(RuntimeError):
    pass


class NotPlugged(RuntimeError):
    pass


class BadDevice(RuntimeError):
    pass


class NotAVolume(RuntimeError):
    # Not a BtrFS volume
    # For example: not btrfs, or normal directory within a btrfs fs
    pass


def path_isprefix(prefix, path):
    # prefix and path must be absolute and normalised,
    # including symlink resolution.
    return prefix == '/' or path == prefix or path.startswith(prefix + '/')


class BtrfsFilesystem2(object):
    """Augments the db-persisted BtrfsFilesystem with some live metadata.
    """

    def __init__(self, whole_fs, impl, uuid):
        self._whole_fs = whole_fs
        self._impl = impl
        self._uuid = uuid
        self._root_info = None
        self._mpoints = None
        self._best_desc = {}
        self._priv_mpoint = None

        self._minfos = None

        try:
            # XXX Not in the db schema yet
            self._impl.label = self.label
        except NotPlugged:
            # XXX No point creating a live object in this case
            pass

    @property
    def impl(self):
        return self._impl

    @property
    def uuid(self):
        return self._uuid

    def iter_open_vols(self):
        for vol in self._whole_fs.iter_open_vols():
            if vol._fs == self:
                yield vol

    def clean_up_mpoints(self):
        if self._priv_mpoint is None:
            return
        for vol in self.iter_open_vols():
            if vol._fd is not None:
                vol.close()
        subprocess.check_call('umount -n -- '.split() + [self._priv_mpoint])
        os.rmdir(self._priv_mpoint)
        self._priv_mpoint = None

    def __str__(self):
        return self.desc

    @memoized_property
    def desc(self):
        try:
            if self.label and self._whole_fs._label_occurs[self.label] == 1:
                return '<%s>' % self.label
        except NotPlugged:
            # XXX Keep the label in the db?
            pass
        return '{%s}' % self.uuid

    def best_desc(self, root_id):
        if root_id not in self._best_desc:
            intpath = self.root_info[root_id].path
            candidate_mis = [
                mi for mi in self.minfos
                if not mi.private and path_isprefix(mi.internal_path, intpath)]
            if candidate_mis:
                mi = max(
                    candidate_mis, key=lambda mi: len(mi.internal_path))
                base = mi.mpoint
                intbase = mi.internal_path
                is_fs_path = True
            else:
                base = self.desc
                intbase = '/'
                is_fs_path = False
            self._best_desc[root_id] = VolDesc(
                os.path.normpath(
                    os.path.join(base, os.path.relpath(intpath, intbase))),
                is_fs_path)
        return self._best_desc[root_id]

    def ensure_private_mpoint(self):
        # Create a private mountpoint with:
        # noatime,noexec,nodev
        # subvol=/
        if self._priv_mpoint is not None:
            return

        self.require_plugged()
        self._whole_fs.ensure_unshared()
        pm = tempfile.mkdtemp(suffix='.privmnt')
        subprocess.check_call(
            'mount -t btrfs -o subvol=/,noatime,noexec,nodev -nU'.split()
            + [str(self.uuid), pm])
        self._priv_mpoint = pm
        self.add_minfo(MountInfo(
            internal_path='/', mpoint=pm, readonly=False, private=True))

    def load_vol_by_root_id(self, root_id):
        self.ensure_private_mpoint()
        ri = self.root_info[root_id]
        return self._whole_fs._get_vol_by_path(
            self._priv_mpoint + ri.path, desc=None)

    @memoized_property
    def root_info(self):
        if not self.minfos:
            raise NotMounted
        fd = os.open(self.minfos[0].mpoint, os.O_DIRECTORY)
        try:
            return read_root_tree(fd)
        finally:
            os.close(fd)

    @memoized_property
    def device_info(self):
        try:
            return self._whole_fs.device_info[self.uuid]
        except KeyError:
            raise NotPlugged(self)

    def require_plugged(self):
        if self.uuid not in self._whole_fs.device_info:
            raise NotPlugged(self)

    @memoized_property
    def label(self):
        return self.device_info.label

    @property
    def minfos(self):
        # Not memoised, some may be added later
        if self._minfos is None:
            mps = []
            try:
                for dev in self.device_info.devices:
                    dev_canonical = os.path.realpath(dev)
                    if dev_canonical in self._whole_fs.mpoints_by_dev:
                        mps.extend(self._whole_fs.mpoints_by_dev[dev_canonical])
            except NotPlugged:
                pass
            self._minfos = mps
        return tuple(self._minfos)

    def add_minfo(self, mi):
        if mi not in self.minfos:
            self._minfos.append(mi)

    def _iter_subvols(self, start_root_ids):
        child_id_map = defaultdict(list)

        for root_id, ri in self.root_info.items():
            if ri.parent_root_id is not None:
                child_id_map[ri.parent_root_id].append(root_id)

        def _iter_children(root_id, top_level):
            yield (root_id, self.root_info[root_id], top_level)
            for child_id in child_id_map[root_id]:
                for item in _iter_children(child_id, False):
                    yield item

        for root_id in start_root_ids:
            for item in _iter_children(root_id, True):
                yield item

    def _load_visible_vols(self, start_paths, nest_desc):
        # Use dicts, there may be repetitions under multiple mountpoints
        loaded = OrderedDict()

        start_vols = OrderedDict(
            (vol.root_id, vol)
            for vol in (
                self._whole_fs._get_vol_by_path(start_fspath, desc=None)
                for start_fspath in start_paths))

        for (root_id, ri, top_level) in self._iter_subvols(start_vols):
            if top_level:
                start_vol = start_vols[root_id]
                if start_vol not in loaded:
                    loaded[start_vol] = True
                start_desc = start_vol.desc
                start_intpath = ri.path
                start_fd = start_vol.fd
                # relpath is more predictable with absolute paths;
                # otherwise it relies on getcwd (via abspath)
                assert os.path.isabs(start_intpath)
            else:
                relpath = os.path.relpath(ri.path, start_intpath)
                if nest_desc:
                    desc = VolDesc(
                        os.path.join(start_desc.description, relpath),
                        start_desc.is_fs_path)
                else:
                    desc = None
                vol = self._whole_fs._get_vol_by_relpath(
                    start_fd, relpath, desc=desc)
                if vol not in loaded:
                    loaded[vol] = True
        return loaded.keys(), start_vols.values()


def impl_property(name):
    def getter(inst):
        return getattr(inst._impl, name)

    def setter(inst, val):
        setattr(inst._impl, name, val)

    return property(getter, setter)


class Volume2(object):
    def __init__(self, whole_fs, fs, impl, desc, fd):
        self._whole_fs = whole_fs
        self._fs = fs
        self._impl = impl
        self._desc = desc
        self._fd = fd

        self.st_dev = os.fstat(self._fd).st_dev

        self._impl.live = self

    last_tracked_generation = impl_property('last_tracked_generation')
    last_tracked_size_cutoff = impl_property('last_tracked_size_cutoff')
    size_cutoff = impl_property('size_cutoff')

    def __str__(self):
        return self.desc.description

    @property
    def impl(self):
        return self._impl

    @property
    def root_info(self):
        return self._fs.root_info[self._impl.root_id]

    @property
    def root_id(self):
        return self._impl.root_id

    @property
    def desc(self):
        return self._desc

    @property
    def fd(self):
        return self._fd

    @property
    def fs(self):
        return self._fs

    @classmethod
    def vol_id_of_fd(cls, fd):
        try:
            return get_fsid(fd), get_root_id(fd)
        except IOError as err:
            if err.errno == errno.ENOTTY:
                raise NotAVolume(fd)
            raise

    def close(self):
        os.close(self._fd)
        self._fd = None

    def lookup_one_path(self, inode):
        return lookup_ino_path_one(self.fd, inode.ino)

    def describe_path(self, relpath):
        return os.path.join(self.desc.description, relpath)


class WholeFS(object):
    """A singleton representing the local filesystem"""

    def __init__(self, sess, size_cutoff=None):
        # Public functions that rely on sess:
        # get_fs, iter_fs, load_all_writable_vols, load_vols,
        # Requiring root:
        # load_all_writable_vols, load_vols.
        self.sess = sess
        self._unshared = False
        self._size_cutoff = size_cutoff
        self._fs_map = {}
        # keyed on fs_uuid, vol.root_id
        self._vol_map = {}
        self._label_occurs = None

    def get_fs_existing(self, uuid):
        assert isinstance(uuid, UUID)
        if uuid not in self._fs_map:
            try:
                db_fs = self.sess.query(
                    BtrfsFilesystem).filter_by(uuid=str(uuid)).one()
            except NoResultFound:
                raise KeyError(uuid)
            fs = BtrfsFilesystem2(self, db_fs, uuid)
            self._fs_map[uuid] = fs
        return self._fs_map[uuid]

    def get_fs(self, uuid):
        assert isinstance(uuid, UUID)
        if uuid not in self._fs_map:
            if uuid in self.device_info:
                db_fs, fs_created = get_or_create(
                    self.sess, BtrfsFilesystem, uuid=str(uuid))
            else:
                # Don't create a db object without a live fs backing it
                try:
                    db_fs = self.sess.query(
                        BtrfsFilesystem).filter_by(uuid=str(uuid)).one()
                except NoResultFound:
                    raise NotPlugged(uuid)
            fs = BtrfsFilesystem2(self, db_fs, uuid)
            self._fs_map[uuid] = fs
        return self._fs_map[uuid]

    def iter_fs(self):
        seen_fs_ids = []
        for (uuid, di) in self.device_info.items():
            fs = self.get_fs(uuid)
            seen_fs_ids.append(fs._impl.id)
            yield fs, di

        extra_fs_query = self.sess.query(BtrfsFilesystem.uuid)
        if seen_fs_ids:
            # Conditional because we get a performance SAWarning otherwise
            extra_fs_query = extra_fs_query.filter(
                ~ BtrfsFilesystem.id.in_(seen_fs_ids))
        for uuid, in extra_fs_query:
            yield self.get_fs(UUID(hex=uuid)), None

    def iter_open_vols(self):
        return iter(self._vol_map.values())

    def _get_vol_by_path(self, volpath, desc):
        volpath = os.path.normpath(volpath)
        fd = os.open(volpath, os.O_DIRECTORY)
        return self._get_vol(fd, desc)

    def _get_vol_by_relpath(self, base_fd, relpath, desc):
        fd = openat(base_fd, relpath, os.O_DIRECTORY)
        return self._get_vol(fd, desc)

    def _get_vol(self, fd, desc):
        if not is_subvolume(fd):
            raise NotAVolume(fd, desc)
        vol_id = Volume2.vol_id_of_fd(fd)

        # If a volume was given multiple times on the command line,
        # keep the first name and fd for it.
        if vol_id in self._vol_map:
            os.close(fd)
            return self._vol_map[vol_id]

        fs_uuid, root_id = vol_id

        fs = self.get_fs(uuid=fs_uuid)
        db_vol, db_vol_created = get_or_create(
            self.sess, Volume, fs=fs._impl, root_id=root_id)

        if self._size_cutoff is not None:
            db_vol.size_cutoff = self._size_cutoff
        elif db_vol_created:
            db_vol.size_cutoff = DEFAULT_SIZE_CUTOFF

        if desc is None:
            desc = fs.best_desc(root_id)

        vol = Volume2(self, fs=fs, impl=db_vol, desc=desc, fd=fd)

        if desc.is_fs_path:
            path_history, ph_created = get_or_create(
                self.sess, VolumePathHistory,
                vol=db_vol, path=desc.description)

        self._vol_map[vol_id] = vol
        return vol

    @memoized_property
    def mpoints_by_dev(self):
        assert not self._unshared
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
                mbd[dev].append(MountInfo(intpath, mpoint, readonly, False))
        return dict(mbd)

    @memoized_property
    def device_info(self):
        di = {}
        lbls = Counter()
        cmd = 'blkid -s LABEL -s UUID -t TYPE=btrfs'.split()
        subp = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for line in subp.stdout:
            dev, label, uuid = BLKID_RE.match(line).groups()
            uuid = UUID(hex=uuid)
            if uuid in di:
                # btrfs raid
                assert di[uuid].label == label
                di[uuid].devices.append(dev)
            else:
                lbls[label] += 1
                di[uuid] = DeviceInfo(label, [dev])
        rc = subp.wait()
        # 2 means there is no btrfs filesystem
        if rc not in (0, 2):
            raise subprocess.CalledProcessError(rc, cmd)
        self._label_occurs = dict(lbls)
        return di

    def ensure_unshared(self):
        if not self._unshared:
            # Make sure we read mountpoints before creating ours,
            # so that ours won't appear on the list.
            self.mpoints_by_dev
            unshare(CLONE_NEWNS)
            self._unshared = True

    def clean_up_mpoints(self):
        if not self._unshared:
            return
        for fs, di in self.iter_fs():
            fs.clean_up_mpoints()

    def close(self):
        # For context managers
        self.clean_up_mpoints()

    def load_vols_for_device(self, devpath, tt):
        for uuid, di in self.device_info.items():
            if any(os.path.samefile(dp, devpath) for dp in di.devices):
                fs = self.get_fs(uuid)
                return self.load_vols_for_fs(fs, tt)
        raise BadDevice('No Btrfs filesystem detected by blkid', devpath)

    def load_vols_for_fs(self, fs, tt):
        # Check that the filesystem is plugged
        fs.device_info

        loaded = []
        fs.ensure_private_mpoint()
        lo, sta = fs._load_visible_vols([fs._priv_mpoint], nest_desc=False)
        assert self._vol_map
        frozen_skipped = 0
        for vol in lo:
            if vol.root_info.is_frozen:
                vol.close()
                frozen_skipped += 1
            else:
                loaded.append(vol)
        if frozen_skipped:
            tt.notify(
                'Skipped %d frozen volumes in filesystem %s' % (
                    frozen_skipped, fs))
        return loaded

    def load_all_writable_vols(self, tt):
        # All non-frozen volumes that are on a
        # filesystem that has a non-ro mountpoint.
        loaded = []
        for (uuid, di) in self.device_info.items():
            fs = self.get_fs(uuid)
            try:
                fs.root_info
            except NotMounted:
                tt.notify('Skipping filesystem %s, not mounted' % fs)
                continue
            if all(mi.readonly for mi in fs.minfos):
                tt.notify('Skipping filesystem %s, not mounted rw' % fs)
                continue
            loaded.extend(self.load_vols_for_fs(fs, tt))
        return loaded

    def load_vols(self, volpaths, tt, recurse):
        # The volume at volpath, plus all its visible non-frozen descendants
        # XXX Some of these may fail if other filesystems
        # are mounted on top of them.
        loaded = OrderedDict()
        for volpath in volpaths:
            vol = self._get_vol_by_path(volpath, desc=VolDesc(volpath, True))
            if recurse:
                if vol.root_info.path != '/':
                    tt.notify(
                        '%s isn\'t the root volume, '
                        'use the filesystem uuid for maximum efficiency.' % vol)
                lo, sta = vol._fs._load_visible_vols([volpath], nest_desc=True)
                skipped = 0
                for vol in lo:
                    if vol in loaded:
                        continue
                    if vol.root_info.is_frozen and vol not in sta:
                        vol.close()
                        skipped += 1
                    else:
                        loaded[vol] = True
                if skipped:
                    tt.notify(
                        'Skipped %d frozen volumes in filesystem %s' % (
                            skipped, vol.fs))
            else:
                if vol not in loaded:
                    loaded[vol] = True
        return loaded.keys()


BLKID_RE = re.compile(
    r'^(?P<dev>/dev/.*):'
    r'(?:\s+LABEL="(?P<label>[^"]*)"|\s+UUID="(?P<uuid>[^"]*)")+\s*$')


def is_subvolume(btrfs_mountpoint_fd):
    st = os.fstat(btrfs_mountpoint_fd)
    return st.st_ino == BTRFS_FIRST_FREE_OBJECTID


def show_fs(fs, print_indented, show_deleted):
    vols_by_id = dict((db_vol.root_id, db_vol) for db_vol in fs._impl.volumes)
    root_ids = set(vols_by_id.keys())
    has_ri = False
    deleted_skipped = 0

    try:
        root_ids.update(fs.root_info.keys())
    except IOError as err:
        if err.errno != errno.EPERM:
            raise
    except NotMounted:
        pass
    else:
        has_ri = True

    for root_id in sorted(root_ids):
        flags = ''
        if has_ri:
            if root_id not in fs.root_info:
                if not show_deleted:
                    deleted_skipped += 1
                    continue
                # The filesystem is available (we could scan the root tree),
                # so the volume must have been destroyed.
                flags = ' (deleted)'
            elif fs.root_info[root_id].is_frozen:
                flags = ' (frozen)'

        print_indented('Volume %d%s' % (root_id, flags), 0)
        try:
            vol = vols_by_id[root_id]
        except KeyError:
            # We'll only use vol in the 'else' no-exception branch
            pass
        else:
            if vol.inode_count:
                print_indented(
                    'As of generation %d, '
                    'tracking %d inodes of size at least %d'
                    % (vol.last_tracked_generation, vol.inode_count,
                       vol.size_cutoff), 1)

        if has_ri and root_id in fs.root_info:
            ri = fs.root_info[root_id]
            desc = fs.best_desc(root_id)
            if desc.is_fs_path:
                print_indented('Accessible at %s' % desc.description, 1)
            else:
                print_indented('Internal path %s' % ri.path, 1)
        else:
            # We can use vol, since keys come from one or the other
            print_indented(
                'Last seen at %s' % vol.last_known_mountpoint, 1)

    if deleted_skipped:
        print_indented('Skipped %d deleted volumes' % deleted_skipped, 0)


def show_vols(whole_fs, fsuuid_or_device, show_deleted):
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
            if fs.uuid == uuid_filter:
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
            show_fs(fs, print_indented, show_deleted)
        elif device_filter is None:
            sys.stdout.write(
                'UUID: %s\n  <no device available>\n' % (fs.uuid,))
            show_fs(fs, print_indented, show_deleted)

    if not found:
        sys.stderr.write(
            'Filesystem at %s was not found\n' % fsuuid_or_device)
    whole_fs.sess.commit()

