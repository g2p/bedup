
import multiprocessing
import os
import shutil
import subprocess
import tempfile

import pytest

from .__main__ import main
from .syncfs import syncfs
from .btrfs import lookup_ino_paths, BTRFS_FIRST_FREE_OBJECTID

# Placate pyflakes
db = fs = fsimage = sampledata = vol_fd = None


def setup_module():
    global db, fs, fsimage, sampledata, vol_fd
    db_fd, db = tempfile.mkstemp(suffix='.sqlite')
    fsimage_fd, fsimage = tempfile.mkstemp(suffix='.btrfs')
    sampledata_fd, sampledata = tempfile.mkstemp(suffix='.sample')
    fs = tempfile.mkdtemp(suffix='.mnt')

    subprocess.check_call(
        'dd bs=4096 count=2048 if=/dev/urandom'.split() + ['of=' + sampledata])
    # The older mkfs.btrfs on travis somehow needs 256M;
    # sparse file, costs nothing
    subprocess.check_call('truncate -s256M --'.split() + [fsimage])
    subprocess.check_call('mkfs.btrfs --'.split() + [fsimage])
    subprocess.check_call('mount -t btrfs -o loop --'.split() + [fsimage, fs])
    shutil.copy(sampledata, os.path.join(fs, 'one.sample'))
    shutil.copy(sampledata, os.path.join(fs, 'two.sample'))
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


def test_functional():
    boxed_call('scan-vol --'.split() + [fs])
    with open(fs + '/one.sample', 'r+') as busy_file:
        boxed_call('dedup-vol --'.split() + [fs])
    boxed_call('forget-vol --'.split() + [fs])
    boxed_call('scan-vol --size-cutoff=65536 --'.split() + [fs, fs])
    boxed_call('dedup-vol --'.split() + [fs])
    boxed_call(
        'dedup-files --defragment --'.split() +
        [fs + '/one.sample', fs + '/two.sample'])
    with open(fs + '/one.sample', 'r+') as busy_file:
        boxed_call(
            'dedup-files --defragment --'.split() +
                [fs + '/one.sample', fs + '/two.sample'],
            expected_rv=1)
    boxed_call('find-new --'.split() + [fs])
    boxed_call('show-vols'.split())


@pytest.mark.xfail
def test_lookup_ino_paths():
    # yeah, crasher. shouldn't happen on those examples though.
    ino = os.stat(os.path.join(fs, 'one.sample')).st_ino
    assert tuple(lookup_ino_paths(vol_fd, ino)) == ('one.sample', )
    assert tuple(
        lookup_ino_paths(vol_fd, BTRFS_FIRST_FREE_OBJECTID)) == ('/', )


def teardown_module():
    if vol_fd is not None:
        os.close(vol_fd)
    try:
        subprocess.check_call('umount --'.split() + [fsimage])
    except subprocess.CalledProcessError:
        # Apparently we kept the vol fd around
        # Not necessarily a bad thing, because keeping references
        # to closed file descriptors is much worse.
        # Will need a test harness that lets us split processes,
        # and still tracks code coverage in the slave.
        subprocess.check_call('lsof -n'.split() + [fs])
        raise
    finally:
        os.unlink(fsimage)
        os.unlink(sampledata)
        os.rmdir(fs)

