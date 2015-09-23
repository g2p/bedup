# vim: set fileencoding=utf-8 sw=4 ts=4 et :

# bedup - Btrfs deduplication
# Copyright (C) 2015 Gabriel de Perthuis <g2p.code+bedup@gmail.com>
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



import argparse
import codecs
import errno
import locale
import os
import sqlalchemy
import sys
import warnings
import xdg.BaseDirectory  # pyxdg, apt:python-xdg

from collections import defaultdict, OrderedDict
from contextlib import closing, ExitStack
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import SingletonThreadPool
from uuid import UUID

from .platform.btrfs import find_new, get_root_generation
from .platform.ioprio import set_idle_priority
from .platform.syncfs import syncfs

from .dedup import dedup_same, FilesInUseError
from .filesystem import show_vols, WholeFS, NotAVolume
from .migrations import upgrade_schema
from .termupdates import TermTemplate
from .tracking import (
    track_updated_files, dedup_tracked, reset_vol, fake_updates,
    annotated_inodes_by_size)


APP_NAME = 'bedup'


def cmd_dedup_files(args):
    try:
        return dedup_same(args.source, args.dests, args.defrag)
    except FilesInUseError as exn:
        exn.describe(sys.stderr)
        return 1


def cmd_find_new(args):
    volume_fd = os.open(args.volume, os.O_DIRECTORY)
    if args.zero_terminated:
        sep = '\0'
    else:
        sep = '\n'
    find_new(volume_fd, args.generation, sys.stdout, terse=args.terse, sep=sep)


def cmd_show_vols(args):
    sess = get_session(args)
    whole_fs = WholeFS(sess)
    show_vols(whole_fs, args.fsuuid_or_device, args.show_deleted)


def sql_setup(dbapi_con, con_record):
    cur = dbapi_con.cursor()
    # Uncripple the SQL implementation
    cur.execute('PRAGMA foreign_keys = ON')
    cur.execute('PRAGMA foreign_keys')
    val = cur.fetchone()
    assert val == (1,), val

    # So that writers do not block readers
    # https://www.sqlite.org/wal.html
    cur.execute('PRAGMA journal_mode = WAL')
    cur.execute('PRAGMA journal_mode')
    val = cur.fetchone()
    # SQLite 3.7 is required
    assert val == ('wal',), val


def get_session(args):
    if args.db_path is None:
        data_dir = xdg.BaseDirectory.save_data_path(APP_NAME)
        args.db_path = os.path.join(data_dir, 'db.sqlite')
    url = sqlalchemy.engine.url.URL('sqlite', database=args.db_path)
    engine = sqlalchemy.engine.create_engine(
        url, echo=args.verbose_sql, poolclass=SingletonThreadPool)
    sqlalchemy.event.listen(engine, 'connect', sql_setup)
    upgrade_schema(engine)
    Session = sessionmaker(bind=engine)
    sess = Session()
    return sess


def vol_cmd(args):
    if args.command == 'dedup-vol':
        sys.stderr.write(
            "The dedup-vol command is deprecated, please use dedup.\n")
        args.command = 'dedup'
        args.defrag = False
    elif args.command == 'reset' and not args.filter:
        sys.stderr.write("You need to list volumes explicitly.\n")
        return 1

    with ExitStack() as stack:
        tt = stack.enter_context(closing(TermTemplate()))
        # Adds about 1s to cold startup
        sess = get_session(args)
        whole_fs = WholeFS(sess, size_cutoff=args.size_cutoff)
        stack.enter_context(closing(whole_fs))

        if not args.filter:
            vols = whole_fs.load_all_writable_vols(tt)
        else:
            vols = OrderedDict()
            for filt in args.filter:
                if filt.startswith('vol:/'):
                    volpath = filt[4:]
                    try:
                        filt_vols = whole_fs.load_vols(
                            [volpath], tt, recurse=False)
                    except NotAVolume:
                        sys.stderr.write(
                            'Path doesn\'t point to a btrfs volume: %r\n'
                            % (volpath,))
                        return 1
                elif filt.startswith('/'):
                    if os.path.realpath(filt).startswith('/dev/'):
                        filt_vols = whole_fs.load_vols_for_device(filt, tt)
                    else:
                        volpath = filt
                        try:
                            filt_vols = whole_fs.load_vols(
                                [volpath], tt, recurse=True)
                        except NotAVolume:
                            sys.stderr.write(
                                'Path doesn\'t point to a btrfs volume: %r\n'
                                % (volpath,))
                            return 1
                else:
                    try:
                        uuid = UUID(hex=filt)
                    except ValueError:
                        sys.stderr.write(
                            'Filter format not recognised: %r\n' % filt)
                        return 1
                    filt_vols = whole_fs.load_vols_for_fs(
                        whole_fs.get_fs(uuid), tt)
                for vol in filt_vols:
                    vols[vol] = True

        # XXX should group by mountpoint instead.
        # Only a problem when called with volume names instead of an fs filter.
        vols_by_fs = defaultdict(list)

        if args.command == 'reset':
            for vol in vols:
                if user_confirmation(
                    'Reset tracking status of {}?'.format(vol), False
                ):
                    reset_vol(sess, vol)
                    print('Reset of {} done'.format(vol))

        if args.command in ('scan', 'dedup'):
            set_idle_priority()
            for vol in vols:
                if args.flush:
                    tt.format('{elapsed} Flushing %s' % (vol,))
                    syncfs(vol.fd)
                    tt.format(None)
                track_updated_files(sess, vol, tt)
                vols_by_fs[vol.fs].append(vol)

        if args.command == 'dedup':
            if args.groupby == 'vol':
                for vol in vols:
                    tt.notify('Deduplicating volume %s' % vol)
                    dedup_tracked(sess, [vol], tt, defrag=args.defrag)
            elif args.groupby == 'mpoint':
                for fs, volset in vols_by_fs.items():
                    tt.notify('Deduplicating filesystem %s' % fs)
                    dedup_tracked(sess, volset, tt, defrag=args.defrag)
            else:
                assert False, args.groupby

        # For safety only.
        # The methods we call from the tracking module are expected to commit.
        sess.commit()


def cmd_generation(args):
    volume_fd = os.open(args.volume, os.O_DIRECTORY)
    if args.flush:
        syncfs(volume_fd)
    generation = get_root_generation(volume_fd)
    print('%d' % generation)


def user_confirmation(message, default):
    # default='n' would be an easy mistake to make
    assert default is bool(default)

    yes_values = 'y yes'.split()
    no_values = 'n no'.split()
    if default:
        choices = 'Y/n'
        yes_values.append('')
    else:
        choices = 'y/N'
        no_values.append('')

    while True:
        try:
            choice = input("%s (%s) " % (message, choices)).lower().strip()
        except EOFError:
            # non-interactive
            choice = ''
        if choice in yes_values:
            return True
        elif choice in no_values:
            return False


def cmd_forget_fs(args):
    sess = get_session(args)
    whole_fs = WholeFS(sess)
    filesystems = [
        whole_fs.get_fs_existing(UUID(hex=uuid)) for uuid in args.uuid]
    for fs in filesystems:
        if not user_confirmation('Wipe all data about fs %s?' % fs, False):
            continue
        for vol in fs._impl.volumes:
            # A lot of things will cascade
            sess.delete(vol)
        sess.delete(fs._impl)
        sess.commit()
        print('Wiped all data about %s' % fs)


def cmd_size_lookup(args):
    sess = get_session(args)
    whole_fs = WholeFS(sess)
    if args.zero_terminated:
        end ='\0'
    else:
        end = '\n'
    for vol, rp, inode in annotated_inodes_by_size(whole_fs, args.size):
        print(vol.describe_path(rp), end=end)

    # We've deleted some stale inodes
    sess.commit()


def cmd_shell(args):
    sess = get_session(args)
    whole_fs = WholeFS(sess)
    from . import model
    try:
        from IPython import embed
    except ImportError:
        sys.stderr.write(
            'Please install bedup[interactive] for this feature\n')
        return 1
    with warnings.catch_warnings():
        warnings.simplefilter('default')
        warnings.filterwarnings('ignore', module='IPython')
        embed()


def cmd_fake_updates(args):
    sess = get_session(args)
    faked = fake_updates(sess, args.max_events)
    sess.commit()
    print('Faked about %d commonality clusters' % faked)


def sql_flags(parser):
    parser.add_argument(
        '--db-path', dest='db_path',
        help='Override the location of the sqlite database')
    parser.add_argument(
        '--verbose-sql', action='store_true', dest='verbose_sql',
        help='Print SQL statements being executed')


def vol_flags(parser):
    parser.add_argument(
        'filter', nargs='*',
        help='List filesystem uuids, devices, or volume mountpoints to '
        'select which volumes are included. '
        'Prefix a volume mountpoint with vol: if you do not want '
        'subvolumes to be included.')
    sql_flags(parser)
    parser.add_argument(
        '--size-cutoff', type=int, dest='size_cutoff',
        help='Change the minimum size (in bytes) of tracked files '
        'for the listed volumes. '
        'Lowering the cutoff will trigger a partial rescan of older files.')
    parser.add_argument(
        '--no-crossvol', action='store_const',
        const='vol', default='mpoint', dest='groupby',
        help='This option disables cross-volume deduplication. '
        'This may be useful with pre-3.6 kernels.')


def scan_flags(parser):
    vol_flags(parser)
    parser.add_argument(
        '--flush', action='store_true', dest='flush',
        help='Flush outstanding data using syncfs before scanning volumes')


def is_in_path(cmd):
    # See shutil.which in Python 3.3
    return any(
        os.path.exists(el + '/' + cmd) for el in os.environ['PATH'].split(':'))


def main(argv):
    progname = 'bedup' if is_in_path('bedup') else 'python3 -m bedup'
    io_enc = codecs.lookup(locale.getpreferredencoding()).name
    if io_enc == 'ascii':
        print(
            'bedup will abort because Python was configured to use ASCII '
            'for console I/O.\nSee https://git.io/vnzk6 which '
              'explains how to use a UTF-8 locale.', file=sys.stderr)
        return 1
    parser = argparse.ArgumentParser(prog=progname)
    parser.add_argument(
        '--debug', action='store_true', help=argparse.SUPPRESS)
    commands = parser.add_subparsers(dest='command', metavar='command')

    sp_scan_vol = commands.add_parser(
        'scan', help='Scan', description="""
Scans volumes to keep track of potentially duplicated files.""")
    sp_scan_vol.set_defaults(action=vol_cmd)
    scan_flags(sp_scan_vol)

    # In Python 3.2+ we can add aliases here.
    # Hidden aliases doesn't seem supported though.
    sp_dedup_vol = commands.add_parser(
        'dedup', help='Scan and deduplicate', description="""
Runs scan, then deduplicates identical files.""")
    sp_dedup_vol.set_defaults(action=vol_cmd)
    scan_flags(sp_dedup_vol)
    sp_dedup_vol.add_argument(
        '--defrag', action='store_true',
        help='Defragment files that are going to be deduplicated')

    # An alias so as not to break btrfs-time-machine.
    # help='' is unset, which should make it (mostly) invisible.
    sp_dedup_vol_compat = commands.add_parser(
        'dedup-vol', description="""
A deprecated alias for the 'dedup' command.""")
    sp_dedup_vol_compat.set_defaults(action=vol_cmd)
    scan_flags(sp_dedup_vol_compat)

    sp_reset_vol = commands.add_parser(
        'reset', help='Reset tracking metadata', description="""
Reset tracking data for the listed volumes. Mostly useful for testing.""")
    sp_reset_vol.set_defaults(action=vol_cmd)
    vol_flags(sp_reset_vol)

    sp_show_vols = commands.add_parser(
        'show', help='Show metadata overview', description="""
Shows filesystems and volumes with their tracking status.""")
    sp_show_vols.set_defaults(action=cmd_show_vols)
    sp_show_vols.add_argument('fsuuid_or_device', nargs='?')
    sp_show_vols.add_argument(
        '--show-deleted', dest='show_deleted', action='store_true',
        help='Show volumes that have been deleted')
    sql_flags(sp_show_vols)

    sp_find_new = commands.add_parser(
        'find-new', help='List changed files', description="""
lists changes to volume since generation

This is a reimplementation of btrfs find-new,
modified to include directories as well.""")
    sp_find_new.set_defaults(action=cmd_find_new)
    sp_find_new.add_argument(
        '-0|--zero-terminated', dest='zero_terminated', action='store_true',
        help='Use a NUL character as the line separator')
    sp_find_new.add_argument(
        '--terse', dest='terse', action='store_true', help='Print names only')
    sp_find_new.add_argument('volume', help='Volume to search')
    sp_find_new.add_argument(
        'generation', type=int, nargs='?', default=0,
        help='Only show items modified at generation or a newer transaction')

    sp_forget_fs = commands.add_parser(
        'forget-fs', help='Wipe all metadata', description="""
Wipe all metadata for the listed filesystems.
Useful if the filesystems don't exist anymore.""")
    sp_forget_fs.set_defaults(action=cmd_forget_fs)
    sp_forget_fs.add_argument('uuid', nargs='+', help='Btrfs filesystem uuids')
    sql_flags(sp_forget_fs)

    sp_dedup_files = commands.add_parser(
        'dedup-files', help='Deduplicate listed', description="""
Freezes listed files, checks them for being identical,
and projects the extents of the first file onto the other files.

The effects are visible with filefrag -v (apt:e2fsprogs),
which displays the extent map of files.
        """.strip())
    sp_dedup_files.set_defaults(action=cmd_dedup_files)
    sp_dedup_files.add_argument('source', metavar='SRC', help='Source file')
    sp_dedup_files.add_argument(
        'dests', metavar='DEST', nargs='+', help='Dest files')
    # Don't forget to also set new options in the dedup-vol test in vol_cmd
    sp_dedup_files.add_argument(
        '--defrag', action='store_true',
        help='Defragment the source file first')

    sp_generation = commands.add_parser(
        'generation', help='Display volume generation', description="""
Display the btrfs generation of VOLUME.""")
    sp_generation.set_defaults(action=cmd_generation)
    sp_generation.add_argument('volume', help='Btrfs volume')
    sp_generation.add_argument(
        '--flush', action='store_true', dest='flush',
        help='Flush outstanding data using syncfs before lookup')

    sp_size_lookup = commands.add_parser(
        'size-lookup', help='Look up inodes by size', description="""
List tracked inodes with a given size.""")
    sp_size_lookup.set_defaults(action=cmd_size_lookup)
    sp_size_lookup.add_argument('size', type=int)
    sp_size_lookup.add_argument(
        '-0|--zero-terminated', dest='zero_terminated', action='store_true',
        help='Use a NUL character as the line separator')
    sql_flags(sp_size_lookup)

    sp_shell = commands.add_parser(
        'shell', description="""
Run an interactive shell (useful for prototyping).""")
    sp_shell.set_defaults(action=cmd_shell)
    sql_flags(sp_shell)

    sp_fake_updates = commands.add_parser(
        'fake-updates', description="""
Fake inode updates from the latest dedup events (useful for benchmarking).""")
    sp_fake_updates.set_defaults(action=cmd_fake_updates)
    sp_fake_updates.add_argument('max_events', type=int)
    sql_flags(sp_fake_updates)

    # Give help when no subcommand is given
    if not argv[1:]:
        parser.print_help()
        return

    args = parser.parse_args(argv[1:])

    if args.debug:
        try:
            from ipdb import launch_ipdb_on_exception
        except ImportError:
            sys.stderr.write(
                'Please install bedup[interactive] for this feature\n')
            return 1
        with launch_ipdb_on_exception():
            # Handle all warnings as errors.
            # Overrides the default filter that ignores deprecations
            # and prints the rest.
            warnings.simplefilter('error')
            warnings.filterwarnings('ignore', module='IPython\..*')
            warnings.filterwarnings('ignore', module='alembic\..*')
            return args.action(args)
    else:
        try:
            return args.action(args)
        except IOError as err:
            if err.errno == errno.EPERM:
                sys.stderr.write(
                    "You need to run this command as root.\n")
                return 1
            raise


def script_main():
    # site.py takes about 1s before main gets called
    sys.exit(main(sys.argv))


if __name__ == '__main__':
    script_main()

