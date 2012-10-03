from cffi import FFI
import os

# Or we could just use psutil (though it's not PyPy compatible)


ffi = FFI()
ffi.cdef('''
#define IOPRIO_WHO_PROCESS ...
#define IOPRIO_WHO_PGRP ...
#define IOPRIO_WHO_USER ...

int ioprio_get(int which, int who);
int ioprio_set(int which, int who, int ioprio);
int IOPRIO_PRIO_VALUE(int class, int data);
int IOPRIO_PRIO_CLASS(int mask);
int IOPRIO_PRIO_DATA(int mask);
''')

# nabbed from schedutils/ionice.c
# include/linux/ioprio.h has the macro half
lib = ffi.verify('''
#include <sys/syscall.h>
#define IOPRIO_CLASS_SHIFT      (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
#define IOPRIO_PRIO_MASK        ((1UL << IOPRIO_CLASS_SHIFT) - 1)
#define IOPRIO_PRIO_CLASS(mask) ((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)  ((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)

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

IOPRIO_IDLE_CLASS = 3


def set_idle_priority(pid=None):
    """
    Puts a process in the idle io priority class.

    If pid is omitted, use the current process.
    """

    if pid is None:
        pid = os.getpid()
    lib.ioprio_set(
        lib.IOPRIO_WHO_PROCESS, pid,
        lib.IOPRIO_PRIO_VALUE(IOPRIO_IDLE_CLASS, 0))

