# vim: set fileencoding=utf-8 sw=4 ts=4 et :

import argparse
import collections
import os
import sqlalchemy
import sys
import xdg.BaseDirectory  # pyxdg, apt:python-xdg

from sqlalchemy.orm import sessionmaker

from .btrfs import find_new
from .dedup import dedup_same
from .ioprio import set_idle_priority
from .model import META
from .tracking import get_vol, track_updated_files, dedup_tracked


APP_NAME = 'bedup'


def cmd_dedup_files(args):
    return dedup_same(args.source, args.dests, args.defragment)


def cmd_find_new(args):
    volume_fd = os.open(args.volume, os.O_DIRECTORY)
    # May raise FindError, let Python print it
    find_new(volume_fd, args.generation, sys.stdout)


def cmd_scan_vol(args):
    return vol_cmd(args, scan_only=True)


def cmd_dedup_vol(args):
    return vol_cmd(args, scan_only=False)


def vol_cmd(args, scan_only):
    data_dir = xdg.BaseDirectory.save_data_path(APP_NAME)
    url = sqlalchemy.engine.url.URL(
        'sqlite', database=os.path.join(data_dir, 'db.sqlite'))
    engine = sqlalchemy.engine.create_engine(url, echo=args.verbose_sql)
    Session = sessionmaker(bind=engine)
    sess = Session()
    META.create_all(engine)

    # Only use the path as a description, it is liable to change.
    volumes = [get_vol(sess, os.open(volpath, os.O_DIRECTORY), desc=volpath)
               for volpath in args.volume]
    vols_by_fs = collections.defaultdict(list)

    set_idle_priority()
    for vol in volumes:
        # May raise IOError
        track_updated_files(sess, vol, sys.stdout, args.verbose_scan)
        vols_by_fs[vol.fs].append(vol)

    if not scan_only:
        for volset in vols_by_fs.itervalues():
            dedup_tracked(sess, volset, sys.stdout)


def vol_flags(parser):
    parser.add_argument('volume', nargs='+', help='btrfs volumes')
    parser.add_argument(
        '--verbose-scan', action='store_true', dest='verbose_scan',
        help='print inodes being scanned')
    parser.add_argument(
        '--verbose-sql', action='store_true', dest='verbose_sql',
        help='print SQL statements being executed')


def main():
    parser = argparse.ArgumentParser(prog='python -m bedup')
    commands = parser.add_subparsers()

    sp_scan_vol = commands.add_parser('scan-vol')
    sp_scan_vol.set_defaults(action=cmd_scan_vol)
    vol_flags(sp_scan_vol)

    sp_dedup_vol = commands.add_parser('dedup-vol')
    sp_dedup_vol.set_defaults(action=cmd_dedup_vol)
    vol_flags(sp_dedup_vol)

    sp_dedup_files = commands.add_parser(
        'dedup-files', description="""
Freezes files, checks them for being identical,
and projects the extents of the first file onto the other files.

The effects are visible with filefrag -v (apt:e2fsprogs),
which displays the extent map of files.
        """.strip())
    sp_dedup_files.set_defaults(action=cmd_dedup_files)
    sp_dedup_files.add_argument('source', metavar='SRC', help='source file')
    sp_dedup_files.add_argument(
        'dests', metavar='DEST', nargs='+', help='dest files')
    sp_dedup_files.add_argument(
        '--defragment', action='store_true',
        help='defragment the source file first')

    sp_find_new = commands.add_parser(
        'find-new', description="""
lists changes to volume since generation

This is a reimplementation of btrfs find-new,
modified to include directories as well.""")
    sp_find_new.set_defaults(action=cmd_find_new)
    sp_find_new.add_argument('volume', help='volume to search')
    sp_find_new.add_argument(
        'generation', type=int, nargs='?', default=0,
        help='only show items modified at generation or a newer transaction')

    args = parser.parse_args()
    return args.action(args)


if __name__ == '__main__':
    sys.exit(main())

