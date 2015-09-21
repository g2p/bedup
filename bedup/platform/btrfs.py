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



import cffi
import os
import posixpath
import sys
import uuid

from ..compat import buffer_to_bytes
from .fiemap import same_extents
from . import cffi_support

from collections import namedtuple


ffi = cffi.FFI()

ffi.cdef("""
/* ioctl.h */

#define BTRFS_IOC_TREE_SEARCH ...
#define BTRFS_IOC_INO_PATHS ...
#define BTRFS_IOC_INO_LOOKUP ...
#define BTRFS_IOC_FS_INFO ...
#define BTRFS_IOC_CLONE ...
#define BTRFS_IOC_DEFRAG ...
#define BTRFS_IOC_SUBVOL_GETFLAGS ...
#define BTRFS_IOC_SUBVOL_SETFLAGS ...

#define BTRFS_FSID_SIZE ...
#define BTRFS_UUID_SIZE ...

struct btrfs_ioctl_search_key {
    /* The search root
    /* tree_id = 0 will use the subvolume from the ioctl fd */
    uint64_t tree_id;

    /* keys returned will be >= min and <= max */
    uint64_t min_objectid;
    uint64_t max_objectid;

    /* keys returned will be >= min and <= max */
    uint64_t min_offset;
    uint64_t max_offset;

    /* max and min transids to search for */
    uint64_t min_transid;
    uint64_t max_transid;

    /* keys returned will be >= min and <= max */
    uint32_t min_type;
    uint32_t max_type;

    /*
     * how many items did userland ask for, and how many are we
     * returning
     */
    uint32_t nr_items;

    ...;
};

struct btrfs_ioctl_search_header {
    uint64_t transid;
    uint64_t objectid;
    uint64_t offset;
    uint32_t type;
    uint32_t len;
};

struct btrfs_ioctl_search_args {
    /* search parameters and state */
    struct btrfs_ioctl_search_key key;
    /* found items */
    char buf[];
};

struct btrfs_data_container {
    uint32_t    bytes_left; /* out -- bytes not needed to deliver output */
    uint32_t    bytes_missing;  /* out -- additional bytes needed for result */
    uint32_t    elem_cnt;   /* out */
    uint32_t    elem_missed;    /* out */
    uint64_t    val[0];     /* out */
};

struct btrfs_ioctl_ino_path_args {
    uint64_t                inum;       /* in */
    uint64_t                size;       /* in */
    /* struct btrfs_data_container  *fspath;       out */
    uint64_t                fspath;     /* out */
    ...; // reserved/padding
};

struct btrfs_ioctl_fs_info_args {
    uint64_t max_id;                /* max device id; out */
    uint64_t num_devices;           /* out */
    uint8_t fsid[16];      /* BTRFS_FSID_SIZE == 16; out */
    ...; // reserved/padding
};

struct btrfs_ioctl_ino_lookup_args {
    uint64_t treeid;
    uint64_t objectid;

    // pads to 4k; don't use this ioctl for path lookup, it's kind of broken.
    // re-enabled, the alternative is buggy atm
    //char name[BTRFS_INO_LOOKUP_PATH_MAX];
    char name[4080];
    //...;
};


/* ctree.h */

#define BTRFS_EXTENT_DATA_KEY ...
#define BTRFS_INODE_REF_KEY ...
#define BTRFS_INODE_ITEM_KEY ...
#define BTRFS_DIR_ITEM_KEY ...
#define BTRFS_DIR_INDEX_KEY ...
#define BTRFS_ROOT_ITEM_KEY ...
#define BTRFS_ROOT_BACKREF_KEY ...

#define BTRFS_FIRST_FREE_OBJECTID ...
#define BTRFS_ROOT_TREE_OBJECTID ...
#define BTRFS_FS_TREE_OBJECTID ...

// A root_item flag
// Not to be confused with a similar ioctl flag with a different value
// XXX The kernel uses cpu_to_le64 to check this flag
#define BTRFS_ROOT_SUBVOL_RDONLY ...


struct btrfs_file_extent_item {
    /*
     * transaction id that created this extent
     */
    uint64_t generation;
    /*
     * max number of bytes to hold this extent in ram
     * when we split a compressed extent we can't know how big
     * each of the resulting pieces will be.  So, this is
     * an upper limit on the size of the extent in ram instead of
     * an exact limit.
     */
    uint64_t ram_bytes;

    /*
     * 32 bits for the various ways we might encode the data,
     * including compression and encryption.  If any of these
     * are set to something a given disk format doesn't understand
     * it is treated like an incompat flag for reading and writing,
     * but not for stat.
     */
    uint8_t compression;
    uint8_t encryption;
    uint16_t other_encoding; /* spare for later use */

    /* are we inline data or a real extent? */
    uint8_t type;

    /*
     * disk space consumed by the extent, checksum blocks are included
     * in these numbers
     */
    uint64_t disk_bytenr;
    uint64_t disk_num_bytes;
    /*
     * the logical offset in file blocks (no csums)
     * this extent record is for.  This allows a file extent to point
     * into the middle of an existing extent on disk, sharing it
     * between two snapshots (useful if some bytes in the middle of the
     * extent have changed
     */
    uint64_t offset;
    /*
     * the logical number of file blocks (no csums included)
     */
    uint64_t num_bytes;
    ...;
};

struct btrfs_timespec {
    uint64_t sec;
    uint32_t nsec;
    ...;
};

struct btrfs_inode_item {
    /* nfs style generation number */
    uint64_t generation;
    /* transid that last touched this inode */
    uint64_t transid;
    uint64_t size;
    uint64_t nbytes;
    uint64_t block_group;
    uint32_t nlink;
    uint32_t uid;
    uint32_t gid;
    uint32_t mode;
    uint64_t rdev;
    uint64_t flags;

    /* modification sequence number for NFS */
    uint64_t sequence;

    struct btrfs_timespec atime;
    struct btrfs_timespec ctime;
    struct btrfs_timespec mtime;
    struct btrfs_timespec otime;
    ...; // reserved/padding
};

struct btrfs_root_item {
// XXX CFFI and endianness: ???
    struct btrfs_inode_item inode;
    uint64_t generation;
    uint64_t root_dirid;
    uint64_t bytenr;
    uint64_t byte_limit;
    uint64_t bytes_used;
    uint64_t last_snapshot;
    uint64_t flags;
    uint32_t refs;
    struct btrfs_disk_key drop_progress;
    uint8_t drop_level;
    uint8_t level;

    /*
     * The following fields appear after subvol_uuids+subvol_times
     * were introduced.
     */

    /*
     * This generation number is used to test if the new fields are valid
     * and up to date while reading the root item. Everytime the root item
     * is written out, the "generation" field is copied into this field. If
     * anyone ever mounted the fs with an older kernel, we will have
     * mismatching generation values here and thus must invalidate the
     * new fields. See btrfs_update_root and btrfs_find_last_root for
     * details.
     * the offset of generation_v2 is also used as the start for the memset
     * when invalidating the fields.
     */
    uint64_t generation_v2;
    //uint8_t uuid[BTRFS_UUID_SIZE]; // BTRFS_UUID_SIZE == 16
    //uint8_t parent_uuid[BTRFS_UUID_SIZE];
    //uint8_t received_uuid[BTRFS_UUID_SIZE];
    uint64_t ctransid; /* updated when an inode changes */
    uint64_t otransid; /* trans when created */
    uint64_t stransid; /* trans when sent. non-zero for received subvol */
    uint64_t rtransid; /* trans when received. non-zero for received subvol */
    struct btrfs_timespec ctime;
    struct btrfs_timespec otime;
    struct btrfs_timespec stime;
    struct btrfs_timespec rtime;
    ...; // reserved and packing
};


struct btrfs_inode_ref {
    uint64_t index;
    uint16_t name_len;
    /* name goes here */
    ...;
};

/*
 * this is used for both forward and backward root refs
 */
struct btrfs_root_ref {
    uint64_t dirid;
    uint64_t sequence;
    uint16_t name_len;
    /* name goes here */
    ...;
};

struct btrfs_disk_key {
    uint64_t objectid;
    uint8_t type;
    uint64_t offset;
    ...;
};

struct btrfs_dir_item {
    struct btrfs_disk_key location;
    uint64_t transid;
    uint16_t data_len;
    uint16_t name_len;
    uint8_t type;
    ...;
};

uint64_t btrfs_stack_file_extent_generation(struct btrfs_file_extent_item *s);
uint64_t btrfs_stack_inode_generation(struct btrfs_inode_item *s);
uint64_t btrfs_stack_inode_size(struct btrfs_inode_item *s);
uint32_t btrfs_stack_inode_mode(struct btrfs_inode_item *s);
uint64_t btrfs_stack_inode_ref_name_len(struct btrfs_inode_ref *s);
uint16_t btrfs_stack_root_ref_name_len(struct btrfs_root_ref *s);
uint64_t btrfs_stack_root_ref_dirid(struct btrfs_root_ref *s);
uint16_t btrfs_stack_dir_name_len(struct btrfs_dir_item *s);
uint64_t btrfs_root_generation(struct btrfs_root_item *s);
""")


# Also accessible as ffi.verifier.load_library()
lib = cffi_support.verify(ffi, '''
    #include <btrfs/ioctl.h>
    #include <btrfs/ctree.h>
    ''',
    include_dirs=[cffi_support.BTRFS_INCLUDE_DIR])


BTRFS_FIRST_FREE_OBJECTID = lib.BTRFS_FIRST_FREE_OBJECTID

u64_max = ffi.cast('uint64_t', -1)

RootInfo = namedtuple('RootInfo', 'path parent_root_id is_frozen')


def name_of_inode_ref(ref):
    namelen = lib.btrfs_stack_inode_ref_name_len(ref)
    return os.fsdecode(ffi.string(ffi.cast('char*', ref + 1), namelen))


def name_of_root_ref(ref):
    namelen = lib.btrfs_stack_root_ref_name_len(ref)
    return os.fsdecode(ffi.string(ffi.cast('char*', ref + 1), namelen))


def name_of_dir_item(item):
    namelen = lib.btrfs_stack_dir_name_len(item)
    return os.fsdecode(ffi.string(ffi.cast('char*', item + 1), namelen))


def ioctl_pybug(fd, ioc, arg=0):
    # Private import
    import fcntl

    if isinstance(arg, int):
        return fcntl.ioctl(fd, ioc, arg)

    # Check for http://bugs.python.org/issue1520818
    # Also known as http://bugs.python.org/issue9758
    # Fixed in 2.7.1, 3.1.3, and 3.2, not backported to 2.6
    # which is now in maintenance mode.
    if len(arg) == 1024:
        raise ValueError(arg)

    return fcntl.ioctl(fd, ioc, arg, True)


def lookup_ino_paths(volume_fd, ino, alloc_extra=0):  # pragma: no cover
    raise OSError('kernel bugs')

    # This ioctl requires root
    args = ffi.new('struct btrfs_ioctl_ino_path_args*')

    assert alloc_extra >= 0
    # XXX We're getting some funky overflows here
    # inode-resolve -v 541144
    # NB: as of 3.6.1 the kernel will allow at most 4096 bytes here,
    # from the min_t in fs/btrfs/ioctl.c
    alloc_size = 4096 + alloc_extra

    # keep a reference around; args.fspath isn't a reference after the cast
    fspath = ffi.new('char[]', alloc_size)

    args.fspath = ffi.cast('uint64_t', fspath)
    args.size = alloc_size
    args.inum = ino

    ioctl_pybug(volume_fd, lib.BTRFS_IOC_INO_PATHS, ffi.buffer(args))
    data_container = ffi.cast('struct btrfs_data_container *', fspath)
    if not (data_container.bytes_missing == data_container.elem_missed == 0):
        print(
            'Problem inode %d %d %d' % (
                ino, data_container.bytes_missing, data_container.elem_missed),
            file=sys.stderr)
        # just say no
        raise IOError('Problem on inode %d' % ino)

        if alloc_extra:
            # We already added a lot of padding, don't get caught in a loop.
            raise IOError('Problem on inode %d' % ino)
        else:
            # The +1024 is some extra padding so we don't have to realloc twice
            # if someone is creating hardlinks while we run.
            # The + 8 * is a workaround for the kernel being a little off
            # in its pointer logic.
            # Want: yield from
            for el in lookup_ino_paths(
                volume_fd, ino,
                data_container.bytes_missing + 1024
                + 8 * data_container.elem_missed):
                yield el
            return

    base = ffi.cast('char*', data_container.val)
    offsets = ffi.cast('uint64_t*', data_container.val)

    for i_path in range(data_container.elem_cnt):
        ptr = base + offsets[i_path]
        path = os.fsdecode(ffi.string(ptr))
        yield path


def get_fsid(fd):
    if False:  # pragma: nocover
        args = ffi.new('struct btrfs_ioctl_fs_info_args *')
        args_buf = ffi.buffer(args)
    else:
        # Work around http://bugs.python.org/issue1520818
        # by making sure the buffer size isn't 1024
        args_cbuf = ffi.new(
            'char[]',
            max(ffi.sizeof('struct btrfs_ioctl_fs_info_args'), 1025))
        args_buf = ffi.buffer(args_cbuf)
        args = ffi.cast('struct btrfs_ioctl_fs_info_args *', args_cbuf)
    before = tuple(args.fsid)
    ioctl_pybug(fd, lib.BTRFS_IOC_FS_INFO, args_buf)
    after = tuple(args.fsid)
    # Check for http://bugs.python.org/issue1520818
    assert after != before, (before, after)
    return uuid.UUID(bytes=buffer_to_bytes(ffi.buffer(args.fsid)))


def get_root_id(fd):
    args = ffi.new('struct btrfs_ioctl_ino_lookup_args *')
    # the inode of the root directory
    args.objectid = lib.BTRFS_FIRST_FREE_OBJECTID
    ioctl_pybug(fd, lib.BTRFS_IOC_INO_LOOKUP, ffi.buffer(args))
    return args.treeid


def lookup_ino_path_one(volume_fd, ino, tree_id=0):
    # tree_id == 0 means the subvolume in volume_fd
    # Sort of sucks (only gets one backref),
    # but that's sufficient for now; the other option
    # has kernel bugs we can't work around.
    args = ffi.new('struct btrfs_ioctl_ino_lookup_args *')
    args.objectid = ino
    args.treeid = tree_id
    ioctl_pybug(volume_fd, lib.BTRFS_IOC_INO_LOOKUP, ffi.buffer(args))
    rv = os.fsdecode(ffi.string(args.name))
    # For some reason the kernel puts a final /
    if tree_id == 0:
        assert rv[-1:] == '/', repr(rv)
        return rv[:-1]
    else:
        return rv


def read_root_tree(volume_fd):
    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key

    sk.tree_id = lib.BTRFS_ROOT_TREE_OBJECTID  # the tree of roots
    sk.max_objectid = u64_max
    sk.min_type = lib.BTRFS_ROOT_ITEM_KEY
    sk.max_type = lib.BTRFS_ROOT_BACKREF_KEY
    sk.max_offset = u64_max
    sk.max_transid = u64_max

    root_info = {}
    ri_rel = {}

    while True:
        sk.nr_items = 4096

        ioctl_pybug(
            volume_fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
        if sk.nr_items == 0:
            break

        offset = 0
        for item_id in range(sk.nr_items):
            sh = ffi.cast(
                'struct btrfs_ioctl_search_header *', args.buf + offset)
            offset += ffi.sizeof('struct btrfs_ioctl_search_header') + sh.len
            if sh.type == lib.BTRFS_ROOT_ITEM_KEY:
                item = ffi.cast('struct btrfs_root_item *', sh + 1)
                is_frozen = bool(item.flags & lib.BTRFS_ROOT_SUBVOL_RDONLY)
                item_root_id = sh.objectid
                if sh.objectid == lib.BTRFS_FS_TREE_OBJECTID:
                    root_info[sh.objectid] = RootInfo('/', None, is_frozen)
            elif sh.type == lib.BTRFS_ROOT_BACKREF_KEY:
                ref = ffi.cast('struct btrfs_root_ref *', sh + 1)
                assert sh.objectid != lib.BTRFS_FS_TREE_OBJECTID
                dir_id = lib.btrfs_stack_root_ref_dirid(ref)
                root_id = sh.objectid
                name = name_of_root_ref(ref)
                # We can use item and is_frozen
                # from the previous loop iteration
                assert root_id == item_root_id
                parent_root_id = sh.offset  # completely obvious, no?
                # The path from the parent root to the parent directory
                reldirpath = lookup_ino_path_one(
                    volume_fd, dir_id, tree_id=parent_root_id)
                assert parent_root_id
                if parent_root_id in root_info:
                    root_info[root_id] = RootInfo(
                        posixpath.join(
                            root_info[parent_root_id].path, reldirpath, name),
                    parent_root_id,
                    is_frozen)
                else:
                    ri_rel[root_id] = RootInfo(
                        posixpath.join(reldirpath, name),
                        parent_root_id,
                        is_frozen)
            # There's also a uuid we could catch on a sufficiently recent
            # BTRFS_ROOT_ITEM_KEY (v3.6). Since the fs is live careful
            # invalidation (in case it was mounted by an older kernel)
            # shouldn't be necessary.

        sk.min_objectid = sh.objectid
        sk.min_type = max(lib.BTRFS_ROOT_ITEM_KEY, sh.type)
        sk.min_offset = sh.offset + 1

    # Deal with parent_root_id > root_id,
    # happens after moving subvolumes.
    while ri_rel:
        for (root_id, ri) in ri_rel.items():
            if ri.parent_root_id not in root_info:
                continue
            parent_path = root_info[ri.parent_root_id].path
            root_info[root_id] = ri._replace(
                path=posixpath.join(parent_path, ri.path))
            del ri_rel[root_id]
    return root_info


def get_root_generation(volume_fd):
    # Adapted from find_root_gen in btrfs-list.c
    # XXX I'm iffy about the search, we may not be using the most
    # recent snapshot, don't want to pick up a newer generation from
    # a different snapshot.
    treeid = get_root_id(volume_fd)
    max_found = 0

    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key

    sk.tree_id = lib.BTRFS_ROOT_TREE_OBJECTID  # the tree of roots
    sk.min_objectid = sk.max_objectid = treeid
    sk.min_type = sk.max_type = lib.BTRFS_ROOT_ITEM_KEY
    sk.max_offset = u64_max
    sk.max_transid = u64_max

    while True:
        sk.nr_items = 4096

        ioctl_pybug(
            volume_fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
        if sk.nr_items == 0:
            break

        offset = 0
        for item_id in range(sk.nr_items):
            sh = ffi.cast(
                'struct btrfs_ioctl_search_header *', args.buf + offset)
            offset += ffi.sizeof('struct btrfs_ioctl_search_header') + sh.len
            assert sh.objectid == treeid
            assert sh.type == lib.BTRFS_ROOT_ITEM_KEY
            item = ffi.cast(
                'struct btrfs_root_item *', sh + 1)
            max_found = max(max_found, lib.btrfs_root_generation(item))

        sk.min_offset = sh.offset + 1

    assert max_found > 0
    return max_found


# clone_data and defragment also have _RANGE variants
def clone_data(dest, src, check_first):
    if check_first and same_extents(dest, src):
        return False
    ioctl_pybug(dest, lib.BTRFS_IOC_CLONE, src)
    return True


def defragment(fd):
    # XXX Can remove compression as a side-effect
    # Also, can unshare extents.
    ioctl_pybug(fd, lib.BTRFS_IOC_DEFRAG)


def find_new(volume_fd, min_generation, results_file, terse, sep):
    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key

    # Not a valid objectid that I know.
    # But find-new uses that and it seems to work.
    sk.tree_id = 0

    sk.min_transid = min_generation

    sk.max_objectid = u64_max
    sk.max_offset = u64_max
    sk.max_transid = u64_max
    sk.max_type = lib.BTRFS_EXTENT_DATA_KEY

    while True:
        sk.nr_items = 4096

        # May raise EPERM
        ioctl_pybug(
            volume_fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)

        if sk.nr_items == 0:
            break

        offset = 0
        for item_id in range(sk.nr_items):
            sh = ffi.cast(
                'struct btrfs_ioctl_search_header *', args.buf + offset)
            offset += ffi.sizeof('struct btrfs_ioctl_search_header') + sh.len

            # XXX The classic btrfs find-new looks only at extents,
            # and doesn't find empty files or directories.
            # Need to look at other types.
            if sh.type == lib.BTRFS_EXTENT_DATA_KEY:
                item = ffi.cast(
                    'struct btrfs_file_extent_item *', sh + 1)
                found_gen = lib.btrfs_stack_file_extent_generation(
                    item)
                if terse:
                    name = lookup_ino_path_one(volume_fd, sh.objectid)
                    results_file.write(name + sep)
                else:
                    results_file.write(
                        'item type %d ino %d len %d gen0 %d gen1 %s%s' % (
                            sh.type, sh.objectid, sh.len, sh.transid,
                            found_gen, sep))
                if found_gen < min_generation:
                    continue
            elif sh.type == lib.BTRFS_INODE_ITEM_KEY:
                item = ffi.cast(
                    'struct btrfs_inode_item *', sh + 1)
                found_gen = lib.btrfs_stack_inode_generation(item)
                if terse:
                    # XXX sh.objectid must be wrong
                    continue
                    name = lookup_ino_path_one(volume_fd, sh.objectid)
                    results_file.write(name + sep)
                else:
                    results_file.write(
                        'item type %d ino %d len %d gen0 %d gen1 %d%s' % (
                            sh.type, sh.objectid, sh.len, sh.transid,
                            found_gen, sep))
                if found_gen < min_generation:
                    continue
            elif sh.type == lib.BTRFS_INODE_REF_KEY:
                ref = ffi.cast(
                    'struct btrfs_inode_ref *', sh + 1)
                name = name_of_inode_ref(ref)
                if terse:
                    # XXX short name
                    continue
                    results_file.write(name + sep)
                else:
                    results_file.write(
                        'item type %d ino %d len %d gen0 %d name %s%s' % (
                            sh.type, sh.objectid, sh.len, sh.transid,
                            name, sep))
            elif (sh.type == lib.BTRFS_DIR_ITEM_KEY
                  or sh.type == lib.BTRFS_DIR_INDEX_KEY):
                item = ffi.cast(
                    'struct btrfs_dir_item *', sh + 1)
                name = name_of_dir_item(item)
                if terse:
                    # XXX short name
                    continue
                    results_file.write(name + sep)
                else:
                    results_file.write(
                        'item type %d dir ino %d len %d'
                        ' gen0 %d gen1 %d type1 %d name %s%s' % (
                            sh.type, sh.objectid, sh.len,
                            sh.transid, item.transid, item.type, name, sep))
            else:
                if not terse:
                    results_file.write(
                        'item type %d oid %d len %d gen0 %d%s' % (
                            sh.type, sh.objectid, sh.len, sh.transid, sep))
        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        # CFFI 0.3 raises an OverflowError if necessary, no need to assert
        #assert sk.min_offset < u64_max
        # If the OverflowError actually happens in practice,
        # we'll need to increase min_type resetting min_objectid to zero,
        # then increase min_objectid resetting min_type and min_offset to zero.
        # See
        # https://btrfs.wiki.kernel.org/index.php/Btrfs_design#Btree_Data_structures
        # and btrfs_key for the btree iteration order.
        sk.min_offset += 1

