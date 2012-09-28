# vim: set fileencoding=utf-8 sw=4 ts=4 et :

import cffi
import fcntl


from os import getcwd  # XXX


ffi = cffi.FFI()

ffi.cdef("""
/* ioctl.h */

#define BTRFS_IOC_TREE_SEARCH ...

struct btrfs_ioctl_search_key {
    /* possibly the root of the search
     * though the ioctl fd seems to be used as well */
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


/* ctree.h */

#define BTRFS_EXTENT_DATA_KEY ...
#define BTRFS_INODE_REF_KEY ...
#define BTRFS_INODE_ITEM_KEY ...
#define BTRFS_DIR_ITEM_KEY ...
#define BTRFS_DIR_INDEX_KEY ...

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

    /*
     * a little future expansion, for more than this we can
     * just grow the inode item and version it
     */
    uint64_t reserved[4];
    struct btrfs_timespec atime;
    struct btrfs_timespec ctime;
    struct btrfs_timespec mtime;
    struct btrfs_timespec otime;
    ...;
};

struct btrfs_inode_ref {
    uint64_t index;
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
uint64_t btrfs_stack_inode_ref_name_len(struct btrfs_inode_ref *s);
uint64_t btrfs_stack_dir_name_len(struct btrfs_dir_item *s);
""")


# Also accessible as ffi.verifier.load_library()
lib = ffi.verify('''
    #include <btrfs-progs/ioctl.h>
    #include <btrfs-progs/ctree.h>
    ''',
    include_dirs=[getcwd()])


u64_max = ffi.cast('uint64_t', -1)


def ino_resolve(volume_fd, ino):
    """
    A straightforward port of ino_resolve in btrfs-progs find-new.

    Requires a search.
    Conceptually broken because of files with multiple hardlinks.
    btrfs does keep track of a preferred name for inodes though.
    """

    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key

    sk.min_objectid = sk.max_objectid = ino
    sk.min_type = sk.max_type = lib.BTRFS_INODE_REF_KEY
    sk.max_offset = u64_max
    sk.max_transid = u64_max
    sk.nr_items = 1

    fcntl.ioctl(volume_fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
    if sk.nr_items == 0:
        return

    sh = ffi.cast(
        'struct btrfs_ioctl_search_header *', args.buf)
    assert sh.type == lib.BTRFS_INODE_REF_KEY
    ref = ffi.cast(
        'struct btrfs_inode_ref *', sh + 1)
    return name_of_inode_ref(ref)


def name_of_inode_ref(ref):
    namelen = lib.btrfs_stack_inode_ref_name_len(ref)
    return ffi.string(ffi.cast('char*', ref + 1), namelen)


def name_of_dir_item(item):
    namelen = lib.btrfs_stack_dir_name_len(item)
    return ffi.string(ffi.cast('char*', item + 1), namelen)


class FindError(Exception):
    pass


def find_new(volume_fd, min_generation, results_file):
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

        try:
            fcntl.ioctl(
                volume_fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
        except IOError as e:
            raise FindError(e)

        if sk.nr_items == 0:
            break

        offset = 0
        for item_id in xrange(sk.nr_items):
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
                # XXX How do we name a hardlinked file?
                #name = ino_resolve(volume_fd, sh.objectid)
                results_file.write(
                    'item type %d ino %d len %d gen0 %d gen1 %d\n' % (
                        sh.type, sh.objectid, sh.len, sh.transid, found_gen))
                if found_gen < min_generation:
                    continue
            elif sh.type == lib.BTRFS_INODE_ITEM_KEY:
                item = ffi.cast(
                    'struct btrfs_inode_item *', sh + 1)
                found_gen = lib.btrfs_stack_inode_generation(item)
                #name = ino_resolve(volume_fd, sh.objectid)
                results_file.write(
                    'item type %d ino %d len %d gen0 %d gen1 %d\n' % (
                        sh.type, sh.objectid, sh.len, sh.transid, found_gen))
                if found_gen < min_generation:
                    continue
            elif sh.type == lib.BTRFS_INODE_REF_KEY:
                ref = ffi.cast(
                    'struct btrfs_inode_ref *', sh + 1)
                name = name_of_inode_ref(ref)
                results_file.write(
                    'item type %d ino %d len %d gen0 %d name %s\n' % (
                        sh.type, sh.objectid, sh.len, sh.transid, name))
            elif (sh.type == lib.BTRFS_DIR_ITEM_KEY
                  or sh.type == lib.BTRFS_DIR_INDEX_KEY):
                item = ffi.cast(
                    'struct btrfs_dir_item *', sh + 1)
                name = name_of_dir_item(item)
                results_file.write(
                    'item type %d dir ino %d len %d'
                    ' gen0 %d gen1 %d type1 %d name %s\n' % (
                        sh.type, sh.objectid, sh.len,
                        sh.transid, item.transid, item.type, name))
            else:
                results_file.write(
                    'item type %d oid %d len %d gen0 %d\n' % (
                        sh.type, sh.objectid, sh.len, sh.transid))
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

