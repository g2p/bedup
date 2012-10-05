
Deduplication for Btrfs.

# Requirements

You need Python 2.7 or PyPy. Other versions of Python aren't
supported due to CFFI incompatibilities.
You need Linux 3.3 or newer.

Install CFFI 0.4. It isn't released yet; this will install it from bitbucket:

    pip install https://bitbucket.org/cffi/cffi/get/tip.tar.gz

Get btrfs-progs (we need the headers at a known location):

    git submodule update --init

There are a few other Python dependencies:

    pip install -e .

# Running

    python -m bedup

You'll see a list of supported commands.

* **scan-vol** scans a subvolume to keep track of potentially duplicated files.
* **dedup-vol** runs scan-vol, then deduplicates identical files.
* **dedup-files** takes a list of identical files and deduplicates them.
* **find-new** is a reimplementation of the `btrfs find-new` command.

To deduplicate a mounted btrfs volume:

    sudo python -m bedup dedup-vol /mnt/btrfs

bedup will not recurse into subvolumes, call it multiple times if necessary.
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

Another limitation of the clone call is that it won't work across subvolumes.
It does have that in common with hard-linking.

