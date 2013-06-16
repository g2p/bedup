
import contextlib
import fcntl
import multiprocessing
import os
import shutil
import subprocess
import tempfile


from .platform.syncfs import syncfs
from .platform.btrfs import lookup_ino_paths, BTRFS_FIRST_FREE_OBJECTID

from .__main__ import main
from . import compat  # monkey-patch check_output and O_CLOEXEC

# Placate pyflakes
tdir = db = fs = fsimage = fsimage2 = sampledata1 = sampledata2 = vol_fd = None


def mk_sample_data(fn):
    subprocess.check_call(
        'dd bs=4096 count=2048 if=/dev/urandom'.split() + ['of=' + fn])
    return fn


def setup_module():
    global tdir, db, fs, fsimage, fsimage2, sampledata1, sampledata2, vol_fd
    tdir = tempfile.mkdtemp(prefix='dedup-tests-')
    db = tdir + '/db.sqlite'
    fsimage = tdir + '/fsimage.btrfs'
    fsimage2 = tdir + '/fsimage-nolabel.btrfs'
    sampledata1 = mk_sample_data(tdir + '/s1.sample')
    sampledata2 = mk_sample_data(tdir + '/s2.sample')
    fs = tdir + '/fs'
    os.mkdir(fs)

    # The older mkfs.btrfs on travis somehow needs 256M;
    # sparse file, costs nothing
    subprocess.check_call('truncate -s256M --'.split() + [fsimage])
    subprocess.check_call('truncate -s256M --'.split() + [fsimage2])
    # mkfs.btrfs is buggy under libefence
    env2 = dict(os.environ)
    if 'LD_PRELOAD' in env2 and 'libefence.so' in env2['LD_PRELOAD']:
        del env2['LD_PRELOAD']
    subprocess.check_call(
        'mkfs.btrfs -LBedupTest --'.split() + [fsimage], env=env2)
    subprocess.check_call(
        'mkfs.btrfs --'.split() + [fsimage2], env=env2)
    subprocess.check_call(
        'mount -t btrfs -o loop,compress-force=lzo --'.split() + [fsimage, fs])
    shutil.copy(sampledata1, os.path.join(fs, 'one.sample'))
    shutil.copy(sampledata1, os.path.join(fs, 'two.sample'))
    shutil.copy(sampledata2, os.path.join(fs, 'three.sample'))
    shutil.copy(sampledata2, os.path.join(fs, 'four.sample'))
    vol_fd = os.open(fs, os.O_DIRECTORY)
    syncfs(vol_fd)


def subp_main(conn, argv):
    try:
        rv = main(argv)
    except Exception as exn:
        conn.send(exn)
        raise
    except:
        conn.send('I don\'t even')
        raise
    else:
        conn.send(rv)


def boxed_call(argv, expected_rv=None):
    # We need multiprocessing rather than fork(), because the
    # former has hooks for nose-cov, pytest-cov & friends.
    # Also fork + sys.exit breaks pytest, os._exit was required.
    # Also also, multiprocessing won't let us use sys.exit either
    # (it captures the exception and changes the exit status).
    # We have to use IPC instead.
    parent_conn, child_conn = multiprocessing.Pipe()
    argv = list(argv)
    if argv[0] not in 'dedup-files find-new'.split():
        argv[1:1] = ['--db-path', db]
    argv[0:0] = ['__main__']
    proc = multiprocessing.Process(target=subp_main, args=(child_conn, argv))
    proc.start()
    rv = parent_conn.recv()
    proc.join()
    if isinstance(rv, Exception):
        raise rv
    assert rv == expected_rv


def stat(fname):
    # stat without args would include ctime, use a custom format
    return subprocess.check_output(
        ['stat', '--printf=atime %x\nmtime %y\n', '--', fname])


@contextlib.contextmanager
def open_cloexec(fname, rw=False):
    if rw:
        fd = os.open(fname, os.O_CLOEXEC | os.O_RDWR)
    else:
        fd = os.open(fname, os.O_CLOEXEC | os.O_RDONLY)
    yield
    os.close(fd)


def test_functional():
    boxed_call('scan --'.split() + [fs])
    with open_cloexec(fs + '/one.sample') as busy1:
        with open_cloexec(fs + '/three.sample') as busy2:
            boxed_call('dedup --'.split() + [fs])
    boxed_call('reset --'.split() + [fs])
    boxed_call('scan --size-cutoff=65536 --'.split() + [fs, fs])
    boxed_call('dedup --'.split() + [fs])
    boxed_call(
        'dedup-files --defrag --'.split() +
        [fs + '/one.sample', fs + '/two.sample'])
    stat0 = stat(fs + '/one.sample')
    shutil.copy(sampledata1, os.path.join(fs, 'two.sample'))
    with open_cloexec(fs + '/one.sample', rw=True):
        with open_cloexec(fs + '/two.sample', rw=True):
            boxed_call(
                'dedup-files --defrag --'.split() +
                    [fs + '/one.sample', fs + '/two.sample'],
                expected_rv=1)
    boxed_call(
        'dedup-files --defrag --'.split() +
            [fs + '/one.sample', fs + '/two.sample'])
    stat1 = stat(fs + '/one.sample')
    # Check that atime and mtime are restored
    assert stat0 == stat1
    boxed_call('find-new --'.split() + [fs])
    boxed_call('show'.split())


def teardown_module():
    if vol_fd is not None:
        os.close(vol_fd)
    try:
        subprocess.check_call('umount --'.split() + [fs])
    except subprocess.CalledProcessError:
        # Apparently we kept the vol fd around
        # Not necessarily a bad thing, because keeping references
        # to closed file descriptors is much worse.
        # Will need a test harness that lets us split processes,
        # and still tracks code coverage in the slave.
        subprocess.check_call('lsof -n'.split() + [fs])
        raise
    finally:
        shutil.rmtree(tdir)

