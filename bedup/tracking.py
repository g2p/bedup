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
import fcntl
import gc
import hashlib
import os
import resource
import stat
import sys
import threading

from collections import defaultdict, namedtuple
from contextlib import closing, contextmanager, ExitStack
from itertools import groupby
from sqlalchemy.sql import and_, select, func, literal_column
from uuid import UUID

from .platform.btrfs import (
    get_root_generation, clone_data, defragment as btrfs_defragment, lib)
from .platform.openat import fopenat, fopenat_rw

from .datetime import system_now
from .dedup import ImmutableFDs, cmp_files
from .filesystem import NotPlugged
from .hashing import mini_hash_from_file, fiemap_hash_from_file
from .model import (
    Inode, get_or_create, DedupEvent, DedupEventInode)


BUFSIZE = 8192

WINDOW_SIZE = 200


def reset_vol(sess, vol):
    # Forgets Inodes, not logging. Make that configurable?
    sess.query(Inode).filter_by(vol=vol.impl).delete()
    vol.last_tracked_generation = 0
    sess.commit()


def fake_updates(sess, max_events):
    faked = 0
    for de in sess.query(DedupEvent).limit(max_events):
        ino_count = 0
        for dei in de.inodes:
            inode = sess.query(Inode).filter_by(
                ino=dei.ino, vol=dei.vol).scalar()
            if not inode:
                continue
            inode.has_updates = True
            ino_count += 1
        if ino_count > 1:
            faked += 1
    sess.commit()
    return faked


def inodes_by_size(sess, size):
    # orm grouping is too complicated, just sort
    return sess.query(Inode).filter_by(size=size).order_by(
        Inode.fs_id, Inode.vol_id)


def annotated_inodes_by_size(whole_fs, size):
    sess = whole_fs.sess
    fs_uuid = None
    vol_id = None

    for inode in inodes_by_size(sess, size):
        if inode.vol_id != vol_id:
            if vol_id is not None and vol is not None:
                vol.close()
            if inode.vol.fs.uuid != fs_uuid:
                if fs_uuid is not None:
                    #fs.close()  # XXX implement
                    fs.clean_up_mpoints()
                fs_uuid = inode.vol.fs.uuid
                fs = whole_fs.get_fs(UUID(hex=fs_uuid))
            vol_id = inode.vol_id
            # XXX Make the mountpoint read-only
            try:
                vol = fs.load_vol_by_root_id(inode.vol.root_id)
            except NotPlugged:
                vol = None
                continue
            except KeyError:
                vol = None
                continue
        if vol is None:
            continue
        try:
            rp = vol.lookup_one_path(inode)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            sess.delete(inode)
            continue
        yield vol, rp, inode


def track_updated_files(sess, vol, tt):
    from .platform.btrfs import ffi, u64_max

    top_generation = get_root_generation(vol.fd)
    if (vol.last_tracked_size_cutoff is not None
        and vol.last_tracked_size_cutoff <= vol.size_cutoff):
        min_generation = vol.last_tracked_generation + 1
    else:
        min_generation = 0
    if min_generation > top_generation:
        tt.notify(
            'Not scanning %s, generation is still %d'
            % (vol, top_generation))
        sess.commit()
        return
    tt.notify(
        'Scanning volume %s generations from %d to %d, with size cutoff %d'
        % (vol, min_generation, top_generation, vol.size_cutoff))
    tt.format(
        '{elapsed} Scanned {scanned} retained {retained:counter}')
    scanned = 0

    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key

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
        for item_id in range(sk.nr_items):
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
                    sess, Inode, vol=vol.impl, ino=ino)
                inode.size = size
                inode.has_updates = True
                tt.update(retained=True)
        scanned += sk.nr_items
        tt.update(scanned=scanned)

        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        sk.min_offset += 1
    tt.format(None)
    vol.last_tracked_generation = top_generation
    vol.last_tracked_size_cutoff = vol.size_cutoff
    sess.commit()


class Checkpointer(threading.Thread):
    def __init__(self, bind):
        super(Checkpointer, self).__init__(name='checkpointer')
        self.bind = bind
        self.evt = threading.Event()
        self.done = False

    def run(self):
        self.conn = self.bind.connect()
        while True:
            self.evt.wait()
            self.conn.execute('PRAGMA wal_checkpoint;')
            self.evt.clear()
            if self.done:
                self.conn.close()
                return

    def please_checkpoint(self):
        self.evt.set()
        if not self.is_alive():
            self.start()

    def close(self):
        if not self.is_alive():
            return
        self.done = True
        self.evt.set()
        self.join()


Commonality1 = namedtuple('Commonality1', 'size inode_count inodes')


class WindowedQuery(object):
    def __init__(
        self, sess, unfiltered, filt_crit, tt, window_size=WINDOW_SIZE
    ):
        self.sess = sess
        self.unfiltered = unfiltered
        self.filt_crit = filt_crit
        self.tt = tt
        self.window_size = window_size

        self.skipped = []

        # select-only, can't be used for updates
        self.filtered_s = filtered = select(
            unfiltered.c
        ).where(
            filt_crit
        ).alias('filtered')

        self.selectable = select([
            filtered.c.size,
            func.count().label('inode_count'),
            func.max(filtered.c.has_updates).label('has_updates')]
        ).group_by(
            filtered.c.size,
        ).having(and_(
            literal_column('inode_count') > 1,
            literal_column('has_updates') > 0,
        ))

        # This is higher than selectable.first().size, in order to also clear
        # updates without commonality.
        self.upper_bound = self.sess.query(
            self.unfiltered.c.size).order_by(
                -self.unfiltered.c.size).limit(1).scalar()
        if self.upper_bound is None:
            self.upper_bound = -1

    def __len__(self):
        return self.sess.execute(self.selectable.count()).scalar()

    def __iter__(self):
        # XXX The PRAGMAs below only work with a SingletonThreadPool.
        # Otherwise they'd have to be re-enabled every time the session
        # calls self.bind.connect().
        # Clearing updates and logging dedup events can cause frequent
        # commits, we don't mind losing them in a crash (no need for
        # durability). SQLite is in WAL mode, so this pragma should disable
        # most commit-time fsync calls without compromising consistency.
        self.sess.execute('PRAGMA synchronous=NORMAL;')
        # Checkpointing is now in the checkpointer thread.
        self.sess.execute('PRAGMA wal_autocheckpoint=0;')
        # just to check commit speed
        #sess.commit()

        checkpointer = Checkpointer(self.sess.bind)
        checkpointer.daemon = True

        # [window_start, window_end] is inclusive at both ends
        selectable = self.selectable.order_by(-self.filtered_s.c.size)

        # This is higher than selectable.first().size, in order to also clear
        # updates without commonality.
        window_start = self.upper_bound

        while True:
            window_select = selectable.where(
                self.filtered_s.c.size <= window_start
            ).limit(self.window_size).alias('s1')
            li = self.sess.execute(window_select).fetchall()
            if not li:
                self.clear_updates(window_start, 0)
                break
            window_start = li[0].size
            window_end = li[-1].size
            # If we wanted to be subtle we'd use limits here as well
            inodes = self.sess.query(Inode).select_entity_from(
                self.filtered_s).join(
                window_select, window_select.c.size == Inode.size
            ).order_by(-Inode.size, Inode.ino)
            inodes_by_size = groupby(inodes, lambda inode: inode.size)
            for size, inodes in inodes_by_size:
                inodes = list(inodes)
                yield Commonality1(size, len(inodes), inodes)
            self.clear_updates(window_start, window_end)
            checkpointer.please_checkpoint()
            window_start = window_end - 1

        self.tt.format('{elapsed} Committing tracking state')
        checkpointer.close()
        # Restore fsync so that the final commit (in dedup_tracked)
        # will be durable.
        self.sess.execute('PRAGMA synchronous=FULL;')

    def clear_updates(self, window_start, window_end):
        # Can't call update directly on FilteredInode because it is aliased.
        # Can't use a <= b <= c in one term with SQLa.
        self.sess.execute(
            self.unfiltered.update().where(and_(
                self.filt_crit,
                window_start >= self.unfiltered.c.size,
                self.unfiltered.c.size >= window_end,
            )).values(
                has_updates=False))

        for inode in self.skipped:
            inode.has_updates = True
        self.sess.commit()
        # clear the list
        self.skipped[:] = []

    def clear_all_updates(self):
        return self.clear_updates(self.upper_bound, 0)


def hardcode_params_unsafe(query):
    # Only tested with ints on sqlite
    # Used to work around the sqlite parameter limit
    q2 = query.compile()
    q2.visit_bindparam = q2.render_literal_bindparam
    return q2.process(query)


def dedup_tracked(sess, volset, tt, defrag):
    fs = volset[0].fs
    vol_ids = [vol.impl.id for vol in volset]
    assert all(vol.fs == fs for vol in volset)

    # 3 for stdio, 3 for sqlite (wal mode), 1 that somehow doesn't
    # get closed, 1 per volume.
    ofile_reserved = 7 + len(volset)

    inode = Inode.__table__
    inode_filt = inode.c.vol_id.in_(vol_ids)
    if len(volset) > 490:
        # SQLite 3 has a hardcoded limit on query parameters
        inode_filt = hardcode_params_unsafe(inode_filt)
    query = WindowedQuery(sess, inode, inode_filt, tt)
    le = len(query)
    ds = DedupSession(sess, tt, defrag, fs, query, ofile_reserved)

    if le:
        # Hopefully close any files we left around
        gc.collect()

        tt.format(
            '{elapsed} Size group {comm1:counter}/{comm1:total} ({size:size}) '
            'sampled {mhash:counter} hashed {fhash:counter} '
            'freed {space_gain:size}')
        tt.set_total(comm1=le)
        for comm1 in query:
            dedup_tracked1(ds, comm1)
        tt.format(None)
    else:
        query.clear_all_updates()
    sess.commit()
    tt.format(None)



class DedupSession(object):
    space_gain = 0

    def __init__(self, sess, tt, defrag, fs, query, ofile_reserved):
        self.sess = sess
        self.tt = tt
        self.defrag = defrag
        self.fs = fs
        self.query = query
        self.ofile_reserved = ofile_reserved
        self.ofile_soft, self.ofile_hard = resource.getrlimit(
            resource.RLIMIT_OFILE)

    def skip(self, inode):
        self.query.skipped.append(inode)

    @contextmanager
    def open_by_inode(self, inode):
        try:
            path = inode.vol.live.lookup_one_path(inode)
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            # Delete stale inodes;
            # some do survive because the number is reused.
            self.sess.delete(inode)
            yield None
            return

        try:
            rfile = fopenat(inode.vol.live.fd, path)
        except IOError as e:
            if e.errno not in (errno.ENOENT, errno.EISDIR):
                raise
            # Don't delete an inode if it was moved by a racing process;
            # mark it for the next run.
            self.skip(inode)
            yield None
            return

        try:
            yield rfile
        finally:
            rfile.close()


def dedup_tracked1(ds, comm1):
    size = comm1.size
    ds.tt.update(comm1=comm1, size=size)
    by_mh = defaultdict(list)
    for inode in comm1.inodes:
        # XXX Need to cope with deleted inodes.
        # We cannot find them in the search-new pass, not without doing
        # some tracking of directory modifications to poke updated
        # directories to find removed elements.

        # rehash everytime for now
        # I don't know enough about how inode transaction numbers are
        # updated (as opposed to extent updates) to be able to actually
        # cache the result
        with ds.open_by_inode(inode) as rfile:
            if rfile is None:
                continue
            try:
                by_mh[mini_hash_from_file(inode, rfile)].append(inode)
            except IOError as e:
                if e.errno == errno.EIO:
                    ds.tt.notify('%r has IO errors, skipping' % inode)
                    continue
                raise
            ds.tt.update(mhash=None)

    for inodes in by_mh.values():
        inode_count = len(inodes)
        if inode_count < 2:
            continue
        fies = set()
        for inode in inodes:
            with ds.open_by_inode(inode) as rfile:
                if rfile is None:
                    continue
                fies.add(fiemap_hash_from_file(rfile))

        if len(fies) < 2:
            continue

        files = []
        fds = []
        # For description only
        fd_names = {}
        fd_inodes = {}
        by_hash = defaultdict(list)

        # XXX I have no justification for doubling inode_count
        ofile_req = 2 * inode_count + ds.ofile_reserved
        if ofile_req > ds.ofile_soft:
            if ofile_req <= ds.ofile_hard:
                resource.setrlimit(
                    resource.RLIMIT_OFILE, (ofile_req, ds.ofile_hard))
                ds.ofile_soft = ofile_req
            else:
                ds.tt.notify(
                    'Too many duplicates (%d at size %d), '
                    'would bring us over the open files limit (%d, %d).'
                    % (inode_count, size, ds.ofile_soft, ds.ofile_hard))
                for inode in inodes:
                    if inode.has_updates:
                        ds.skip(inode)
                continue

        for inode in inodes:
            # Open everything rw, we can't pick one for the source side
            # yet because the crypto hash might eliminate it.
            # We may also want to defragment the source.
            try:
                path = inode.vol.live.lookup_one_path(inode)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    ds.sess.delete(inode)
                    continue
                raise
            try:
                afile = fopenat_rw(inode.vol.live.fd, path)
            except IOError as e:
                if e.errno == errno.ETXTBSY:
                    # The file contains the image of a running process,
                    # we can't open it in write mode.
                    ds.tt.notify('File %r is busy, skipping' % path)
                elif e.errno == errno.EACCES:
                    # Could be SELinux or immutability
                    ds.tt.notify('Access denied on %r, skipping' % path)
                elif e.errno == errno.ENOENT:
                    # The file was moved or unlinked by a racing process
                    ds.tt.notify('File %r may have moved, skipping' % path)
                else:
                    raise
                ds.skip(inode)
                continue

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

            # With a false positive, some kind of cmp pass that compares
            # all files at once might be more efficient that hashing.
            for afile in files:
                fd = afile.fileno()
                inode = fd_inodes[fd]
                if fd in immutability.fds_in_write_use:
                    ds.tt.notify('File %r is in use, skipping' % fd_names[fd])
                    ds.skip(inode)
                    continue
                hasher = hashlib.sha1()
                try:
                    for buf in iter(lambda: afile.read(BUFSIZE), b''):
                        hasher.update(buf)
                except OSError as e:
                    if e.errno == errno.EIO:
                        continue
                    raise

                # Gets rid of a race condition
                st = os.fstat(fd)
                if st.st_ino != inode.ino:
                    ds.skip(inode)
                    continue
                if st.st_dev != inode.vol.live.st_dev:
                    ds.skip(inode)
                    continue

                size1 = afile.tell()
                if size1 != size:
                    if size1 < inode.vol.size_cutoff:
                        # if we didn't delete this inode, it would cause
                        # spurious comm groups in all future invocations.
                        ds.sess.delete(inode)
                    else:
                        ds.skip(inode)
                    continue

                by_hash[hasher.digest()].append(afile)
                ds.tt.update(fhash=None)

            for fileset in by_hash.values():
                dedup_fileset(ds, fileset, fd_names, fd_inodes, size)


def dedup_fileset(ds, fileset, fd_names, fd_inodes, size):
    if len(fileset) < 2:
        return
    sfile = fileset[0]
    sfd = sfile.fileno()
    sdesc = fd_inodes[sfd].vol.live.describe_path(fd_names[sfd])
    if ds.defrag:
        btrfs_defragment(sfd)
    dfiles = fileset[1:]
    dfiles_successful = []
    for dfile in dfiles:
        dfd = dfile.fileno()
        ddesc = fd_inodes[dfd].vol.live.describe_path(
            fd_names[dfd])
        if not cmp_files(sfile, dfile):
            # Probably a bug since we just used a crypto hash
            ds.tt.notify('Files differ: %r %r' % (sdesc, ddesc))
            assert False, (sdesc, ddesc)
            return
        try:
            deduped = clone_data(dest=dfd, src=sfd, check_first=True)
        except IOError as e:
            if e.errno == errno.EINVAL:
                ds.tt.notify(
                    'Error deduplicating, maybe a file is marked NODATACOW:\n'
                    '- %r\n- %r' % (sdesc, ddesc))
                return
            raise
        if deduped:
            ds.tt.notify(
                'Deduplicated:\n- %r\n- %r' % (sdesc, ddesc))
            dfiles_successful.append(dfile)
            ds.space_gain += size
            ds.tt.update(space_gain=ds.space_gain)
        elif False:
            # Often happens when there are multiple files with
            # the same extents, plus one with the same size and
            # mini-hash but a difference elsewhere.
            # We hash the same extents multiple times, but
            # I assume the data is shared in the vfs cache.
            ds.tt.notify(
                'Did not deduplicate (same extents): %r %r' % (
                    sdesc, ddesc))
    if dfiles_successful:
        evt = DedupEvent(
            fs=ds.fs.impl, item_size=size, created=system_now())
        ds.sess.add(evt)
        for afile in [sfile] + dfiles_successful:
            inode = fd_inodes[afile.fileno()]
            evti = DedupEventInode(
                event=evt, ino=inode.ino, vol=inode.vol)
            ds.sess.add(evti)
        ds.sess.commit()

