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
import os

from . import cffi_support


ffi = FFI()
ffi.cdef('''
    int openat(int dirfd, const char *pathname, int flags);
''')
lib = cffi_support.verify(ffi, '''
#include <fcntl.h>
''')


def openat(base_fd, path, flags):
    fd = lib.openat(base_fd, os.fsencode(path), flags)
    if fd < 0:
        # There's a little bit of magic here:
        # IOError.errno is only set if there are exactly two or three
        # arguments.
        raise IOError(ffi.errno, os.strerror(ffi.errno), (base_fd, path))
    return fd


def fopenat(base_fd, path):
    """
    Does openat read-only, then does fdopen to get a file object
    """

    return os.fdopen(openat(base_fd, path, os.O_RDONLY), 'rb')


def fopenat_rw(base_fd, path):
    """
    Does openat read-write, then does fdopen to get a file object
    """

    return os.fdopen(openat(base_fd, path, os.O_RDWR), 'rb+')

