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
// New mount namespace
#define CLONE_NEWNS ...

int unshare(int flags);
''')
lib = cffi_support.verify(ffi, '''
#include <sched.h>
''',
    extra_compile_args=['-D_GNU_SOURCE'])

CLONE_NEWNS = lib.CLONE_NEWNS


def unshare(flags):
    if lib.unshare(flags) != 0:
        raise IOError(ffi.errno, os.strerror(ffi.errno), flags)

