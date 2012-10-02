# vim: set fileencoding=utf-8 sw=4 ts=4 et :

import os
from btrfs import clone_data
from chattr import editflags, FS_IMMUTABLE_FL


BUFSIZE = 8192


class FilesDiffer(ValueError):
    pass


def cmp_fds(fd1, fd2):
    # Python 3 can take closefd=False instead of a duplicated fd.
    fi1 = os.fdopen(os.dup(fd1), 'r')
    fi2 = os.fdopen(os.dup(fd2), 'r')

    while True:
        b1 = fi1.read(BUFSIZE)
        b2 = fi2.read(BUFSIZE)
        if b1 != b2:
            return False
        if not b1:
            return True


def dedup_same(source, dests):
    source_fd = os.open(source, os.O_RDONLY)
    dest_fds = [os.open(dname, os.O_RDWR) for dname in dests]
    return dedup_same_fds(source_fd, dest_fds)


def dedup_same_fds(source_fd, dest_fds):
    revert_immutable_fds = []

    try:
        for fd in [source_fd] + dest_fds:
            # Prevents anyone else from creating write-mode file descriptors,
            # but the ones we just created remain valid.
            was_immutable = editflags(fd, add_flags=FS_IMMUTABLE_FL)
            if not was_immutable:
                revert_immutable_fds.append(fd)
            # TODO: check no one else has kept writable
            # file descriptors around.

        for fd in dest_fds:
            if not cmp_fds(source_fd, fd):
                # XXX FDs are not very descriptive
                raise FilesDiffer(source_fd, fd)
            clone_data(dest=fd, src=source_fd)
    finally:
        for fd in revert_immutable_fds:
            editflags(fd, remove_flags=FS_IMMUTABLE_FL)

