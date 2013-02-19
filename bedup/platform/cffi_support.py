# vim: set fileencoding=utf-8 sw=4 ts=4 et :

# bedup - Btrfs deduplication
# Copyright (C) 2012 Gabriel de Perthuis <g2p.code+bedup@gmail.com>
#
# This file is part of bedup.
#
# bedup is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# bedup is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with bedup.  If not, see <http://www.gnu.org/licenses/>.

import hashlib

from os import getcwd  # XXX


# This will be switched to True by a build_py preprocessor in setup.py
CFFI_INSTALLED_MODE = False
# Will be hardcoded in by a preprocessor so that CFFI module hashes don't
# change after installation
BTRFS_INCLUDE_DIR = getcwd()


def verify(ffi, source, **kwargs):
    assert 'ext_package' not in kwargs
    assert 'modulename' not in kwargs
    kwargs['ext_package'] = 'bedup.platform'

    # modulename can't prevent a rebuild atm,
    # and is also hard to make work with build_ext (build_ext looks
    # at the unprocessed module). Skip it for now.
    if CFFI_INSTALLED_MODE and False:
        # We still need a hash so that the modules have distinct names
        srchash = hashlib.sha1(source).hexdigest()
        kwargs['modulename'] = 'pyext_' + srchash
    return ffi.verify(source, **kwargs)


def get_mods():
    from . import (
        btrfs, chattr, fiemap, futimens, ioprio, openat, syncfs, time, unshare)

    return (
        btrfs, chattr, fiemap, futimens, ioprio, openat, syncfs, time, unshare)


def get_ext_modules():
    return [mod.ffi.verifier.get_extension() for mod in get_mods()]

