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


# Or we could just use psutil (though it's not PyPy compatible)


ffi = FFI()
ffi.cdef('''
#define IOPRIO_WHO_PROCESS ...
#define IOPRIO_WHO_PGRP ...
#define IOPRIO_WHO_USER ...

#define IOPRIO_CLASS_NONE ...
#define IOPRIO_CLASS_RT ...
#define IOPRIO_CLASS_BE ...
#define IOPRIO_CLASS_IDLE ...

int ioprio_get(int which, int who);
int ioprio_set(int which, int who, int ioprio);
int IOPRIO_PRIO_VALUE(int class, int data);
int IOPRIO_PRIO_CLASS(int mask);
int IOPRIO_PRIO_DATA(int mask);
''')

# Parts nabbed from schedutils/ionice.c
# include/linux/ioprio.h has the macro half
lib = cffi_support.verify(ffi, '''
#include <unistd.h>
#include <sys/syscall.h>

#define IOPRIO_CLASS_SHIFT      (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
#define IOPRIO_PRIO_MASK        ((1UL << IOPRIO_CLASS_SHIFT) - 1)
#define IOPRIO_PRIO_CLASS(mask) ((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)  ((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)

enum {
    IOPRIO_CLASS_NONE,
    IOPRIO_CLASS_RT,
    IOPRIO_CLASS_BE,
    IOPRIO_CLASS_IDLE,
};

enum {
    IOPRIO_WHO_PROCESS = 1,
    IOPRIO_WHO_PGRP,
    IOPRIO_WHO_USER,
};

static inline int ioprio_set(int which, int who, int ioprio) {
    return syscall(SYS_ioprio_set, which, who, ioprio);
}

static inline int ioprio_get(int which, int who) {
    return syscall(SYS_ioprio_get, which, who);
}
''')


def set_idle_priority(pid=None):
    """
    Puts a process in the idle io priority class.

    If pid is omitted, applies to the current process.
    """

    if pid is None:
        pid = os.getpid()
    lib.ioprio_set(
        lib.IOPRIO_WHO_PROCESS, pid,
        lib.IOPRIO_PRIO_VALUE(lib.IOPRIO_CLASS_IDLE, 0))

