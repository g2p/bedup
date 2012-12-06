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

import collections
import string
import sys

from .platform.time import monotonic_time

_formatter = string.Formatter()

# Yay VT100
LINE_START = '\r'
CLEAR_END_OF_LINE = '\x1b[K'
CLEAR_LINE = LINE_START + CLEAR_END_OF_LINE
# XXX nowrap doesn't work well in screen (over gnome-term, libvte)
# with some non-ascii characters that are twice as wide in a monospace font.
# All tested terms (urxvt, aterm, xterm, xfce4-terminal, gnome-terminal)
# work fine without screen, for either value of VTE_CJK_WIDTH.
# See: CJK double-width/bi-width
TTY_NOWRAP = '\x1b[?7l'
TTY_DOWRAP = '\x1b[?7h'
HIDE_CURSOR = '\x1b[?25l'
SHOW_CURSOR = '\x1b[?25h'


def format_duration(seconds):
    sec_format = '%05.2f'
    minutes, seconds = divmod(seconds, 60)
    if minutes:
        sec_format = '%04.1f'
    hours, minutes = divmod(minutes, 60)
    if hours:
        sec_format = '%02d'
    days, hours = divmod(hours, 24)
    weeks, days = divmod(days, 7)
    greatest_unit = (
        not weeks, not days, not hours, not minutes, not seconds, False
    ).index(False)
    rv = ''
    if weeks:
        rv += '%dW' % weeks
    if days:
        rv += '%dD' % days
    if rv:
        rv += ' '
    if greatest_unit <= 2:
        rv += '%02d:' % hours
    if greatest_unit <= 3:
        rv += '%02d:' % minutes
    rv += sec_format % seconds
    return rv


class TermTemplate(object):
    def __init__(self):
        self._template = None
        self._kws = {}
        self._kws_counter = collections.defaultdict(int)
        self._kws_totals = {}
        self._stream = sys.stdout
        self._isatty = self._stream.isatty()
        # knowing this is stdout:
        self._newline_needs_flush = not self._isatty
        self._wraps = True

    def update(self, **kwargs):
        self._kws.update(kwargs)
        for key in kwargs:
            self._kws_counter[key] += 1
        self._render(with_newline=False)

    def set_total(self, **kwargs):
        self._kws_totals.update(kwargs)
        self._render(with_newline=False)

    def format(self, template):
        if self._template is not None:
            self._render(with_newline=True)
        else:
            self._initial_time = monotonic_time()
        self._kws.clear()
        self._kws_counter.clear()
        self._kws_totals.clear()
        if template is None:
            self._template = None
        else:
            self._template = tuple(_formatter.parse(template))
            self._time = monotonic_time()
            self._render(with_newline=False)

    def _write_tty(self, data):
        if self._isatty:
            self._stream.write(data)

    def _nowrap(self):
        # Don't forget to flush
        if self._wraps:
            self._write_tty(TTY_NOWRAP)
            self._wraps = False

    def _dowrap(self):
        # Don't forget to flush
        if not self._wraps:
            self._write_tty(TTY_DOWRAP)
            self._wraps = True

    def _render(self, with_newline, flush_anyway=False):
        if (self._template is not None) and (self._isatty or with_newline):
            self._nowrap()
            self._write_tty(CLEAR_LINE)
            for (
                literal_text, field_name, format_spec, conversion
            ) in self._template:
                self._stream.write(literal_text)
                if field_name:
                    if format_spec == '':
                        if field_name in ('elapsed', 'elapsed_total'):
                            format_spec = 'time'

                    if format_spec == '':
                        self._stream.write(str(self._kws.get(field_name, '')))
                    elif format_spec == 'total':
                        if field_name in self._kws_totals:
                            self._stream.write(
                                '%d' % self._kws_totals[field_name])
                        else:
                            self._stream.write('??')
                    elif format_spec == 'time':
                        if field_name == 'elapsed':
                            duration = monotonic_time() - self._time
                        elif field_name == 'elapsed_total':
                            duration = monotonic_time() - self._initial_time
                        else:
                            assert False, field_name
                        self._stream.write(format_duration(duration))
                    elif format_spec == 'truncate-left':
                        # XXX NotImplemented
                        self._stream.write(self._kws.get(field_name, ''))
                    elif format_spec == 'size':
                        # XXX stub
                        self._stream.write(
                            '%d' % (self._kws.get(field_name, 0)))
                    elif format_spec == 'counter':
                        self._stream.write(
                            '%d' % self._kws_counter[field_name])
                    else:
                        assert False, format_spec
            # Just in case we get an inopportune SIGKILL, reset this
            # immediately (before the render flush) so we don't have to
            # rely on finish: clauses or context managers.
            self._dowrap()
            if with_newline:
                self._stream.write('\n')
                if self._newline_needs_flush:
                    self._stream.flush()
            else:
                self._stream.flush()
        elif flush_anyway:
            self._stream.flush()

    def notify(self, message):
        self._write_tty(CLEAR_LINE)
        self._dowrap()
        self._stream.write(message + '\n')
        self._render(
            with_newline=False, flush_anyway=self._newline_needs_flush)

    def close(self):
        # Called close so it can be used with contextlib.closing
        self._render(with_newline=True)
        self._dowrap()
        self._stream.flush()
        self._stream = None

