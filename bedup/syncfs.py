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


ffi = FFI()
ffi.cdef('''
    int syncfs(int fd);
    ''')
lib = ffi.verify('''
    #include <unistd.h>
    ''', ext_package='bedup')


def syncfs(fd):
    if lib.syncfs(fd) != 0:
        raise IOError(ffi.errno, os.strerror(ffi.errno), fd)

