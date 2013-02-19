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

__all__ = ('monotonic_time', )

from cffi import FFI

from . import cffi_support


ffi = FFI()
ffi.cdef('''
#define CLOCK_MONOTONIC ...

// From /usr/include/bits:
// time_t is long, clockid_t is int

struct timespec {
    long     tv_sec;        /* seconds */
    long     tv_nsec;       /* nanoseconds */
};

int clock_gettime(int clk_id, struct timespec *tp);
''')

lib = cffi_support.verify(ffi, '''
#include <time.h>''',
    libraries=['rt'])


def monotonic_time():
    tp = ffi.new('struct timespec *')
    if lib.clock_gettime(lib.CLOCK_MONOTONIC, tp) != 0:
        assert False, ffi.errno
    return tp.tv_sec + 1e-9 * tp.tv_nsec

