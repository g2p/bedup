# bedup - Btrfs deduplication
# Copyright (C) 2012 Gabriel de Perthuis <g2p.code+bedup@gmail.com>
# Copyright (C) 2011 Victor Stinner
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

import codecs
import subprocess
import sys


PY3 = sys.version_info[0] >= 3
FSENC = sys.getfilesystemencoding()


# CFFI 0.4 reimplements Python 2 buffers on Python 3
if PY3 and False:
    def buffer_to_bytes(buf):
        return buf.tobytes()
else:
    def buffer_to_bytes(buf):
        return buf[:]


if PY3:
    _unichr = chr
else:
    _unichr = unichr


try:
    codecs.lookup_error('surrogateescape')
except LookupError:
    def my_se(exc):
        """
        Pure Python implementation of PEP 383: the "surrogateescape" error
        handler of Python 3.1.

        https://bitbucket.org/haypo/misc/src/tip/python/surrogateescape.py
        """
        if isinstance(exc, UnicodeDecodeError):
            decoded = []
            for ch in exc.object[exc.start:exc.end]:
                if PY3:
                    code = ch
                else:
                    code = ord(ch)
                if 0x80 <= code <= 0xFF:
                    decoded.append(_unichr(0xDC00 + code))
                elif code <= 0x7F:
                    decoded.append(_unichr(code))
                else:
                    print("RAISE!")
                    raise exc
            decoded = str().join(decoded)
            return (decoded, exc.end)
        else:
            print(exc.args)
            ch = exc.object[exc.start:exc.end]
            code = ord(ch)
            if not 0xDC80 <= code <= 0xDCFF:
                print("RAISE!")
                raise exc
            print(exc.start)
            byte = _unichr(code - 0xDC00)
            print(repr(byte))
            return (byte, exc.end)

    codecs.register_error('surrogateescape', my_se)


if PY3:
    from os import fsdecode
else:
    def fsdecode(filename):
        assert isinstance(filename, str)
        return filename.decode(FSENC, 'surrogateescape')


if not hasattr(subprocess, 'check_output'):
    # Monkey-patching. That way lies madness.

    def _check_output(*popenargs, **kwargs):
        """Run command with arguments and return its output as a byte string"""

        process = subprocess.Popen(
            stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            error = subprocess.CalledProcessError(retcode, cmd)
            error.output = output
            raise error
        return output
    subprocess.check_output = _check_output

