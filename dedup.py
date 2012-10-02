# vim: set fileencoding=utf-8 sw=4 ts=4 et :

import collections
import errno
import glob
import operator
import os
import re

from btrfs import clone_data
from chattr import editflags, FS_IMMUTABLE_FL


BUFSIZE = 8192


class FilesDifferError(ValueError):
    pass


class FilesInUseError(RuntimeError):
    pass


def cmp_fds(fd1, fd2):
    # Python 3 can take closefd=False instead of a duplicated fd.
    fi1 = os.fdopen(os.dup(fd1), 'r')
    fi2 = os.fdopen(os.dup(fd2), 'r')
    return cmp_files(fi1, fi2)


def cmp_files(fi1, fi2):
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


PROC_PATH_RE = re.compile(r'^/proc/(\d+)/fd/(\d+)$')
FLAGS_LINE_RE = re.compile(r'^flags:\s+0(\d+)\n$')


def find_inodes_in_write_use(fds):
    for (fd, other_pid, other_fd, mode) in find_inodes_in_use(fds):
        if mode & (os.O_WRONLY | os.O_RDWR):
            yield (fd, other_pid, other_fd, mode)


def find_inodes_in_use(fds):
    """
    Find which of these inodes are in use, and give their open modes.

    Does not give the modes of the same fds in the same process,
    but might include other modes if this process has the same file
    open under a different file descriptor.
    """

    self_pid = os.getpid()
    id_fd_assoc = collections.defaultdict(list)

    for fd in fds:
        st = os.fstat(fd)
        id_fd_assoc[(st.st_dev, st.st_ino)].append(fd)

    for proc_path in glob.glob('/proc/*/fd/*'):
        # access the current process under its full pid only
        if proc_path.startswith('/proc/self/'):
            continue

        try:
            st = os.stat(proc_path)
        except OSError, e:
            # glob opens directories during matching,
            # and other processes might close their fds in the meantime.
            # This isn't a problem for the immutable-locked use case.
            if e.errno == errno.ENOENT:
                continue
            raise

        st_id = (st.st_dev, st.st_ino)
        if st_id not in id_fd_assoc:
            continue

        other_pid, other_fd = map(
            int, PROC_PATH_RE.match(proc_path).groups())
        original_fds = id_fd_assoc[st_id]
        if other_pid == self_pid:
            if other_fd in original_fds:
                continue

        try:
            flags_line = list(open('/proc/%d/fdinfo/%d' % (other_pid, other_fd)))[1]
        except IOError, e:
            if e.errno == errno.ENOENT:
                continue
            raise

        # Parse octal
        flags = int(FLAGS_LINE_RE.match(flags_line).group(1), 8)
        for fd in original_fds:
            yield (fd, other_pid, other_fd, flags)


def dedup_same_fds(source_fd, dest_fds):
    return mass_dedup_fds([[source_fd] + dest_fds])


def mass_dedup_fds(fd_sets):
    revert_immutable_fds = []
    fds = reduce(operator.add, fd_sets)

    try:
        for fd in fds:
            # Prevents anyone else from creating write-mode file descriptors,
            # but the ones we just created remain valid.
            was_immutable = editflags(fd, add_flags=FS_IMMUTABLE_FL)
            if not was_immutable:
                revert_immutable_fds.append(fd)
            # TODO: check no one else has kept writable
            # file descriptors around.
            # fuser fails in case of nlinks > 1, lsof does manage to find
            # by name in that case.
            # That basically requires stating all of /proc/*/fd/*
            # and comparing inode numbers.

        in_use = list(find_inodes_in_write_use(fds))
        if in_use:
            raise FilesInUseError(
                'Some of the files to deduplicate '
                'are open for writing elsewhere',
                in_use)

        for fd_set in fd_sets:
            source_fd = fd_set[0]
            dest_fds = fd_set[1:]

            for fd in dest_fds:
                if not cmp_fds(source_fd, fd):
                    # XXX FDs are not very descriptive
                    # OTOH they are lightweight.
                    # Error translation in a non-fd wrapper is an option.
                    raise FilesDifferError(source_fd, fd)
                clone_data(dest=fd, src=source_fd)
    finally:
        for fd in revert_immutable_fds:
            editflags(fd, remove_flags=FS_IMMUTABLE_FL)

