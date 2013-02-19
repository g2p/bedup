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
import weakref

from . import cffi_support


# XXX All this would work effortlessly in Python 3.3:
# st_atime_ns, and os.utime(ns=())


ffi = FFI()
ffi.cdef('''
struct timespec {
    // time_t is long
    long tv_sec;  // seconds
    long tv_nsec; // nanoseconds
};

struct stat {
    struct timespec st_atim;
    struct timespec st_mtim;
    ...;
};

int fstat(int fd, struct stat *buf);

int futimens(int fd, const struct timespec times[2]);
''')
lib = cffi_support.verify(ffi, '''
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
''')


_stat_ownership = weakref.WeakKeyDictionary()


def fstat_ns(fd):
    stat = ffi.new('struct stat *')
    if lib.fstat(fd, stat) != 0:
        raise IOError(ffi.errno, os.strerror(ffi.errno), fd)
    # The nested structs seem to be recreated at every member access.
    atime, mtime = stat.st_atim, stat.st_mtim
    assert 0 <= atime.tv_nsec < 1e9
    assert 0 <= mtime.tv_nsec < 1e9
    _stat_ownership[atime] = _stat_ownership[mtime] = stat
    return atime, mtime


def futimens(fd, ns):
    """
    set inode atime and mtime

    ns is (atime, mtime), a pair of struct timespec
    with nanosecond resolution.
    """

    # ctime can't easily be reset
    # also, we have no way to do mandatory locking without
    # changing the ctime.
    times = ffi.new('struct timespec[2]')
    atime, mtime = ns
    assert 0 <= atime.tv_nsec < 1e9
    assert 0 <= mtime.tv_nsec < 1e9
    times[0] = atime
    times[1] = mtime
    if lib.futimens(fd, times) != 0:
        raise IOError(
            ffi.errno, os.strerror(ffi.errno),
            (fd, atime.tv_sec, atime.tv_nsec, mtime.tv_sec, mtime.tv_nsec))

