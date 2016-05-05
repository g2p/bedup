Deduplication for Btrfs.

bedup looks for new and changed files, making sure that multiple copies of
identical files share space on disk. It integrates deeply with btrfs so that
scans are incremental and low-impact.

Requirements
============

You need Python 3.3 or newer, and Linux 3.3 or newer.
Linux 3.9.4 or newer is recommended, because it fixes a scanning bug
and is compatible with cross-volume deduplication.

This should get you started on Ubuntu 16.04:

::

    sudo aptitude install python3-pip python3-dev python3-cffi libffi-dev build-essential git

This should get you started on earlier versions of Debian/Ubuntu:

::

    sudo aptitude install python3-pip python3-dev libffi-dev build-essential git

This should get you started on Fedora:

::

    yum install python3-pip python3-devel libffi-devel gcc git

Installation
============

On systems other than Ubuntu 16.04 you need to install CFFI:

::

    pip3 install --user cffi

Option 1 (recommended): from a git clone
----------------------------------------

Enable submodules (this will pull headers from btrfs-progs)

::

    git submodule update --init

Complete the installation. This will compile some code with CFFI and
pull the rest of our Python dependencies:

::

    python3 setup.py install --user
    cp -lt ~/bin ~/.local/bin/bedup

Option 2: from a PyPI release
-----------------------------

::

    pip3 install --user bedup
    cp -lt ~/bin ~/.local/bin/bedup

Running
=======

::

    bedup --help
    bedup <command> --help

On Debian and Fedora, you may need to use `sudo -E ~/bin/bedup` or install cffi
and bedup as root (bedup and its dependencies will get installed to /usr/local).

You'll see a list of supported commands.

- **scan** scans volumes to keep track of potentially duplicated files.
- **dedup** runs scan, then deduplicates identical files.
- **show** shows btrfs filesystems and their tracking status.
- **dedup-files** takes a list of identical files and deduplicates them.
- **find-new** reimplements the ``btrfs subvolume find-new`` command
  with a few extra options.

To deduplicate all filesystems: ::

    sudo bedup dedup

Unmounted or read-only filesystems are excluded if they aren't listed
on the command line.
Filesystems can be referenced by uuid or by a path in /dev: ::

    sudo bedup dedup /dev/disks/by-label/Btrfs

Giving a subvolume path also works, and will include subvolumes by default.

Since cross-subvolume deduplication requires Linux 3.6, users of older
kernels should use the ``--no-crossvol`` flag.

Hacking
=======

::

   pip3 install --user pytest tox ipdb https://github.com/jbalogh/check

To run the tests::

   sudo python3 -m pytest -s bedup

To test compatibility and packaging as well::

   GETROOT=/usr/bin/sudo tox

Run a style check on edited files::

   check.py

Implementation
==============

Deduplication is implemented using a Btrfs feature that allows for
cloning data from one file to the other. The cloned ranges become shared
on disk, saving space.

File metadata isn't affected, and later changes to one file won't affect
the other (this is unlike hard-linking).

This approach doesn't require special kernel support, but it has two
downsides: locking has to be done in userspace, and there is no way to
free space within read-only (frozen) snapshots.

Scanning
--------

Scanning is done incrementally, the technique is similar to ``btrfs subvolume
find-new``.  You need an up-to-date kernel (3.10, 3.9.4, 3.8.13.1, 3.6.11.5,
3.5.7.14, 3.4.47) to index all files; earlier releases have a bug that
causes find-new to end prematurely.  The fix can also be cherry-picked
from `this commit
<https://git.kernel.org/cgit/linux/kernel/git/stable/linux-stable.git/patch/?id=514b17caf165ec31d1f6b9d40c645aed55a0b721>`_.

Locking
-------

Before cloning, we need to lock the files so that their contents don't
change from the time the data is compared to the time it is cloned.
Implementation note: This is done by setting the immutable attribute on
the file, scanning /proc to see if some processes still have write
access to the file (via preexisting file descriptors or memory
mappings), bailing if the file is in write use. If all is well, the
comparison and cloning steps can proceed. The immutable attribute is
then reverted.

This locking process might not be fool-proof in all cases; for example a
malicious application might manage to bypass it, which would allow it to
change the contents of files it doesn't have access to.

There is also a small time window when an application will get
permission errors, if it tries to get write access to a file we have
already started to deduplicate.

Finally, a system crash at the wrong time could leave some files immutable.
They will be reported at the next run; fix them using the ``chattr -i``
command.

Subvolumes
----------

The clone call is considered a write operation and won't work on
read-only snapshots.

Before Linux 3.6, the clone call didn't work across subvolumes.

Defragmentation
---------------

Before Linux 3.9, defragmentation could break copy-on-write sharing,
which made it inadvisable when snapshots or deduplication are used.
Btrfs defragmentation has to be explicitly requested (or background
defragmentation enabled), so this generally shouldn't be a problem for
users who were unaware of the feature.

Users of Linux 3.9 or newer can safely pass the `--defrag` option to
`bedup dedup`, which will defragment files before deduplicating them.

Reporting bugs
==============

Be sure to mention the following:

- Linux kernel version: uname -rv
- Python version
- Distribution

And give some of the program output.

Build status
============

.. image:: https://travis-ci.org/g2p/bedup.png
   :target: https://travis-ci.org/g2p/bedup

