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

from zlib import adler32
from . import fiemap


def mini_hash_from_file(inode, rfile):
    # A very cheap, very partial hash for quick disambiguation
    # Won't help with things like zeroed or sparse files.
    # The mini_hash for those is 0x10000001
    rfile.seek(int(inode.size * .3))
    # bitops to make unsigned, for better readability
    return adler32(rfile.read(4096)) & 0xffffffff


def fiemap_hash_from_file(rfile):
    extents = tuple(fiemap.fiemap(rfile.fileno()))
    return hash(extents)

