
Deduplication for Btrfs.

[![Build status with Travis](https://secure.travis-ci.org/g2p/bedup.png)](http://travis-ci.org/g2p/bedup)

# Requirements

You need Python 2.7 (recommended), Python 2.6, Python 3.2, or PyPy.
You need Linux 3.3 or newer.

This should get you started on Debian/Ubuntu:

    sudo aptitude install python-pip python-dev libffi-dev build-essential git

# Installation

Install CFFI 0.4. It isn't released yet; this will install
a development snapshot from bitbucket:

    pip install --user https://bitbucket.org/cffi/cffi/get/default.tar.gz

## Option 1 (recommended): from a git clone

Get btrfs-progs (we need the headers at a known location):

    git submodule update --init

Complete the installation.
This will compile some code with CFFI and pull the rest
of our Python dependencies:

    pip install --user -e .

## Option 2: from a PyPI release

    pip install --user bedup

# Running

    python -m bedup

You'll see a list of supported commands.

* **scan-vol** scans a subvolume to keep track of potentially duplicated files.
* **dedup-vol** runs scan-vol, then deduplicates identical files.
* **dedup-files** takes a list of identical files and deduplicates them.
* **show-vols** shows all known btrfs filesystems and their tracking status.
* **find-new** is a reimplementation of the `btrfs find-new` command.

To deduplicate a mounted btrfs volume:

    sudo python -m bedup dedup-vol /mnt/btrfs

bedup will not recurse into subvolumes, mention multiple subvolumes
on the command line if you want cross-subvolume deduplication (requires
Linux 3.6).
You can get a list of btrfs subvolumes with:

    sudo btrfs subvolume list /mnt/btrfs

The first run can take some time.
Subsequent runs will only scan and deduplicate
the files that have changed in the interval.

# Caveats

Deduplication is currently implemented using a Btrfs feature that
allows for cloning data from one file to the other. The cloned ranges
become shared on disk, saving space. File metadata isn't affected, and
later changes to one file won't affect the other (this is unlike hard-linking).

## Locking

Before cloning, we need to lock the files so that their contents don't change
from the time the data is compared to the time it is cloned.
Implementation note:
This is done by setting the immutable attribute on the file, scanning /proc
to see if some processes still have write access to the file (via preexisting
file descriptors or memory mappings), bailing if the file is in write use.
If all is well, the comparison and cloning steps can proceed. The immutable
attribute is then reverted.

This locking process might not be fool-proof in all cases;
for example a malicious application might manage to bypass it,
which would allow it to change the contents of files it doesn't have
access to.

There is also a small time window when an application will get permission
errors, if it tries to get write access to a file we have already
started to deduplicate.

Finally, a system crash at the wrong time could leave some files immutable;
fix them using the `chattr -i` command.

## Subvolumes

The clone call is considered a write operation, it won't work on read-only
snapshots.

Before Linux 3.6, the clone call didn't work across subvolumes.

## GNU Screen

If you have file names with double-width CJK characters and you use screen,
the display will be a bit messed up (with extraneous line jumps after those
file names).

