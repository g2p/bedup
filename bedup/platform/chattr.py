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

from cffi import FFI
import fcntl

from . import cffi_support


__all__ = (
    'getflags',
    'editflags',
    'FS_IMMUTABLE_FL',
)

ffi = FFI()
ffi.cdef('''
#define FS_IOC_GETFLAGS ...
#define FS_IOC_SETFLAGS ...

#define	FS_SECRM_FL ... /* Secure deletion */
#define	FS_UNRM_FL ... /* Undelete */
#define	FS_COMPR_FL ... /* Compress file */
#define FS_SYNC_FL ... /* Synchronous updates */
#define FS_IMMUTABLE_FL ... /* Immutable file */
#define FS_APPEND_FL ... /* writes to file may only append */
#define FS_NODUMP_FL ... /* do not dump file */
#define FS_NOATIME_FL ... /* do not update atime */
/* Reserved for compression usage... */
#define FS_DIRTY_FL ...
#define FS_COMPRBLK_FL ... /* One or more compressed clusters */
#define FS_NOCOMP_FL ... /* Don't compress */
//#define FS_ECOMPR_FL ... /* Compression error */
/* End compression flags --- maybe not all used */
#define FS_BTREE_FL ... /* btree format dir */
#define FS_INDEX_FL ... /* hash-indexed directory */
#define FS_IMAGIC_FL ... /* AFS directory */
#define FS_JOURNAL_DATA_FL ... /* Reserved for ext3 */
#define FS_NOTAIL_FL ... /* file tail should not be merged */
#define FS_DIRSYNC_FL ... /* dirsync behaviour (directories only) */
#define FS_TOPDIR_FL ... /* Top of directory hierarchies*/
#define FS_EXTENT_FL ... /* Extents */
//#define FS_DIRECTIO_FL ... /* Use direct i/o */
#define FS_NOCOW_FL ... /* Do not cow file */
#define FS_RESERVED_FL ... /* reserved for ext2 lib */

#define FS_FL_USER_VISIBLE ... /* User visible flags */
#define FS_FL_USER_MODIFIABLE ... /* User modifiable flags */
''')

# apt:linux-libc-dev
lib = cffi_support.verify(ffi, '''
    #include <linux/fs.h>
    ''')

FS_IMMUTABLE_FL = lib.FS_IMMUTABLE_FL


def getflags(fd):
    """
    Gets per-file filesystem flags.
    """

    flags_ptr = ffi.new('uint64_t*')
    flags_buf = ffi.buffer(flags_ptr)
    fcntl.ioctl(fd, lib.FS_IOC_GETFLAGS, flags_buf)
    return flags_ptr[0]


def editflags(fd, add_flags=0, remove_flags=0):
    """
    Sets and unsets per-file filesystem flags.
    """

    if add_flags & remove_flags != 0:
        raise ValueError(
            'Added and removed flags shouldn\'t overlap',
            add_flags, remove_flags)

    # The ext2progs code uses int or unsigned long,
    # the kernel uses an implicit int,
    # let's be explicit here.
    flags_ptr = ffi.new('uint64_t*')
    flags_buf = ffi.buffer(flags_ptr)
    fcntl.ioctl(fd, lib.FS_IOC_GETFLAGS, flags_buf)
    prev_flags = flags_ptr[0]
    flags_ptr[0] |= add_flags
    # Python represents negative numbers with an infinite number of
    # ones in bitops, so this will work correctly.
    flags_ptr[0] &= ~remove_flags
    fcntl.ioctl(fd, lib.FS_IOC_SETFLAGS, flags_buf)
    return prev_flags & (add_flags | remove_flags)

