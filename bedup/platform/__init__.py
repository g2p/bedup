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

from . import (
    btrfs, chattr, fiemap, futimens, ioprio, openat, syncfs, time, unshare)

MODS = (btrfs, chattr, fiemap, futimens, ioprio, openat, syncfs, time, unshare)

def get_ext_modules():
    return [mod.ffi.verifier.get_extension() for mod in MODS]

