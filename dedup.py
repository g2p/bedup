# vim: set fileencoding=utf-8 sw=4 ts=4 et :

import collections
import errno
import glob
import os
import re

from btrfs import clone_data
from chattr import editflags, FS_IMMUTABLE_FL


BUFSIZE = 8192


class FilesDifferError(ValueError):
    pass


class FilesInUseError(RuntimeError):
    pass


class UseInfo(collections.namedtuple('UseInfo', 'other_pid other_fd mode')):
    @property
    def is_writable(self):
        return bool(self.mode & (os.O_WRONLY | os.O_RDWR))

    @property
    def is_readable(self):
        return not (self.mode & os.O_WRONLY)


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
    fds = [source_fd] + dest_fds
    fd_names = dict(zip(fds, [source] + dests))

    with ImmutableFDs(fds) as immutability:
        if immutability.fds_in_write_use:
            raise FilesInUseError(
                'Some of the files to deduplicate '
                'are open for writing elsewhere',
                dict(
                    (fd_names[fd], tuple(immutability.write_use_info(fd)))
                    for fd in immutability.fds_in_write_use))

        for fd in dest_fds:
            if not cmp_fds(source_fd, fd):
                raise FilesDifferError(fd_names[source_fd], fd_names[fd])
            clone_data(dest=fd, src=source_fd)


PROC_PATH_RE = re.compile(r'^/proc/(\d+)/fd/(\d+)$')
FLAGS_LINE_RE = re.compile(r'^flags:\s+0(\d+)\n$')


def find_inodes_in_write_use(fds):
    for (fd, use_info) in find_inodes_in_use(fds):
        if use_info.is_writable:
            yield (fd, use_info)


def find_inodes_in_use(fds):
    """
    Find which of these inodes are in use, and give their open modes.

    Does not give the modes of the same fds in the same process,
    but might include other modes if this process has the same file
    open under a different file descriptor.

    XXX This does not catch mmaped files.
    lsof finds mmaps, fuser doesn't.
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
            flags_line = list(open(
                '/proc/%d/fdinfo/%d' % (other_pid, other_fd)))[1]
        except IOError, e:
            if e.errno == errno.ENOENT:
                continue
            raise

        # Parse octal
        mode = int(FLAGS_LINE_RE.match(flags_line).group(1), 8)
        for fd in original_fds:
            yield (fd, UseInfo(other_pid, other_fd, mode))


class ImmutableFDs(object):
    """A context manager to mark a set of fds immutable.

    Actually works at the inode level, fds are just to make sure
    inodes can be referenced unambiguously.
    """

    def __init__(self, fds):
        self.__fds = fds
        self.__revert_list = []
        self.__in_use = None
        self.__writable_fds = None

    def __enter__(self):
        for fd in self.__fds:
            # Prevents anyone from creating write-mode file descriptors,
            # but the ones that already exist remain valid.
            was_immutable = editflags(fd, add_flags=FS_IMMUTABLE_FL)
            if not was_immutable:
                self.__revert_list.append(fd)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for fd in self.__revert_list:
            editflags(fd, remove_flags=FS_IMMUTABLE_FL)

    def __require_use_info(self):
        # We only track write use, other uses can appear after the /proc scan
        if self.__in_use is None:
            self.__in_use = collections.defaultdict(list)
            for (fd, use_info) in find_inodes_in_write_use(self.__fds):
                self.__in_use[fd].append(use_info)
            self.__writable_fds = frozenset(self.__in_use.keys())

    def write_use_info(self, fd):
        self.__require_use_info()
        # A quick check to prevent unnecessary list instanciation
        if fd in self.__in_use:
            return tuple(self.__in_use[fd])
        else:
            return tuple()

    @property
    def fds_in_write_use(self):
        self.__require_use_info()
        return self.__writable_fds

