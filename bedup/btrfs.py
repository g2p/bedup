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

import cffi


from os import getcwd  # XXX


ffi = cffi.FFI()

ffi.cdef("""
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
    //uint64_t disk_bytenr;
    //uint64_t disk_num_bytes;
    /*
     * the logical offset in file blocks (no csums)
     * this extent record is for.  This allows a file extent to point
     * into the middle of an existing extent on disk, sharing it
     * between two snapshots (useful if some bytes in the middle of the
     * extent have changed
     */
    //uint64_t offset;
    /*
     * the logical number of file blocks (no csums included)
     */
    //uint64_t num_bytes;
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
    ...; // reserved and packing
};


struct btrfs_inode_ref {
    uint64_t index;
    //uint16_t name_len;
    /* name goes here */
    ...;
};

struct btrfs_disk_key {
    uint64_t objectid;
    //uint8_t type;
    //uint64_t offset;
    ...;
};

struct btrfs_dir_item {
    struct btrfs_disk_key location;
    //uint64_t transid;
    uint16_t data_len;
    uint16_t name_len;
    uint8_t type;
    ...;
};

// These functions must be present to trigger the bug.
uint64_t btrfs_stack_file_extent_generation(struct btrfs_file_extent_item *s);
uint64_t btrfs_stack_inode_generation(struct btrfs_inode_item *s);
uint64_t btrfs_stack_inode_size(struct btrfs_inode_item *s);
uint32_t btrfs_stack_inode_mode(struct btrfs_inode_item *s);
uint64_t btrfs_stack_inode_ref_name_len(struct btrfs_inode_ref *s);
uint64_t btrfs_stack_dir_name_len(struct btrfs_dir_item *s);

uint64_t btrfs_root_generation(struct btrfs_root_item *s);
""")


# Also accessible as ffi.verifier.load_library()
lib = ffi.verify('''
    #include <btrfs-progs/ctree.h>
    ''',
    ext_package='bedup',
    include_dirs=[getcwd()])

if __name__ == '__main__':
    # Fails in Python3.3 and every Python3-compatible CFFI version I have
    # tested.  If at first it doesn't fail, try again; failure seems to involve
    # a memory error.
    lib.btrfs_root_generation(ffi.new('struct btrfs_root_item *'))

