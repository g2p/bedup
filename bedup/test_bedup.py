
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
fs = fsimage = sampledata = vol_fd = None


def setup():
    global fsimage, fs, sampledata, vol_fd
    fsimage_fd, fsimage = tempfile.mkstemp(suffix='.btrfs')
    sampledata_fd, sampledata = tempfile.mkstemp(suffix='.sample')
    fs = tempfile.mkdtemp(suffix='.mnt')

    subprocess.check_call(
        'dd bs=4096 count=2048 if=/dev/urandom'.split() + ['of=' + sampledata])
    subprocess.check_call('truncate -s64M --'.split() + [fsimage])
    subprocess.check_call('mkfs.btrfs --'.split() + [fsimage])
    subprocess.check_call('mount -o loop --'.split() + [fsimage, fs])
    shutil.copy(sampledata, os.path.join(fs, 'one.sample'))
    shutil.copy(sampledata, os.path.join(fs, 'two.sample'))
    vol_fd = os.open(fs, os.O_DIRECTORY)
    syncfs(vol_fd)


def boxed_call(*argv):
    # We need multiprocessing rather than fork(), because the
    # former has hooks for nose-cov, pytest-cov & friends.
    # Also fork + sys.exit breaks pytest, os._exit was required.
    proc = multiprocessing.Process(target=main, args=(('__main__',) + argv,))
    proc.start()
    proc.join()
    assert proc.exitcode == 0


def test_functional():
    boxed_call('scan-vol', '--', fs)
    boxed_call('dedup-vol', '--', fs)
    boxed_call('forget-vol', '--', fs)
    boxed_call('scan-vol', '--size-cutoff=65536', '--', fs, fs)
    with open(os.path.join(fs, 'one.sample'), 'r+') as busy_file:
        boxed_call('dedup-vol', '--', fs)
    boxed_call(
        'dedup-files', '--defragment', '--',
        fs + '/one.sample', fs + '/two.sample')
    boxed_call('find-new', '--', fs)
    boxed_call('show-vols')


@pytest.mark.xfail
def test_lookup_ino_paths():
    # yeah, crasher. shouldn't happen on those examples though.
    ino = os.stat(os.path.join(fs, 'one.sample')).st_ino
    assert tuple(lookup_ino_paths(vol_fd, ino)) == ('one.sample', )
    assert tuple(lookup_ino_paths(vol_fd, BTRFS_FIRST_FREE_OBJECTID)) == ('/', )


def teardown():
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

