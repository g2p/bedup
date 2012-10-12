
import multiprocessing
import os
import shutil
import subprocess
import tempfile

from .__main__ import main
from .syncfs import syncfs

fs = fsimage = sampledata = None


def setup():
    global fsimage, fs, sampledata
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
    fs_fd = os.open(fs, os.O_DIRECTORY)
    syncfs(fs_fd)
    os.close(fs_fd)


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
    boxed_call('scan-vol', '--', fs)
    boxed_call('dedup-vol', '--', fs)
    boxed_call('find-new', '--', fs)
    boxed_call('show-vols')


def teardown():
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

