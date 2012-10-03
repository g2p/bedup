from cffi import FFI
import os


ffi = FFI()
ffi.cdef('''
    int openat(int dirfd, const char *pathname, int flags);
''')
lib = ffi.verify('''
    #include <fcntl.h>
    ''')


def fopenat(fd, path):
    """
    Does openat read-only, then does fdopen to get a file object
    """

    fd1 = lib.openat(fd, path, os.O_RDONLY)
    if fd1 < 0:
        raise IOError(ffi.errno, os.strerror(ffi.errno))
    return os.fdopen(fd1)


def fopenat_rw(fd, path):
    """
    Does openat read-write, then does fdopen to get a file object
    """

    fd1 = lib.openat(fd, path, os.O_RDWR)
    if fd1 < 0:
        raise IOError(ffi.errno, os.strerror(ffi.errno))
    return os.fdopen(fd1, 'r+')


