Deduplication for Btrfs.

bedup looks for new and changed files, making sure that multiple copies of
identical files share space on disk. It integrates deeply with btrfs so that
scans are incremental and low-impact.

Requirements
============

You need Python 2.7 (recommended), Python 3.3, Python 3.2, or PyPy. You
need Linux 3.3 or newer, with glibc 2.14 or newer.

This should get you started on Debian/Ubuntu:

::

    sudo aptitude install python-pip python-dev libffi-dev build-essential git

Installation
============

Install CFFI 0.4.2.

::

    pip install --user cffi

Option 1 (recommended): from a git clone
----------------------------------------

Get btrfs-progs (we need the headers at a known location):

::

    git submodule update --init

Complete the installation. This will compile some code with CFFI and
pull the rest of our Python dependencies:

::

    python setup.py install --user
    cp -lt ~/bin ~/.local/bin/bedup

Option 2: from a PyPI release
-----------------------------

::

    pip install --user bedup
    cp -lt ~/bin ~/.local/bin/bedup

Running
=======

::

    bedup -h

If bedup isn't in your path or your sudo path, use ``python -m bedup`` instead.

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

   pip install --user pytest tox ipdb https://github.com/jbalogh/check

To run the tests::

   sudo py.test -s bedup

To test compatibility and packaging as well::

   tox

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

Build status
============

.. image:: https://travis-ci.org/g2p/bedup.png
   :target: https://travis-ci.org/g2p/bedup

