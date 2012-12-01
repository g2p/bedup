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

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData

from .model import META


REV = 1


def upgrade_with_range(context, from_rev, to_rev):
    assert from_rev == to_rev
    op = Operations(context)
    #from IPython import embed; embed()


def upgrade_schema(engine, database_exists):
    context = MigrationContext.configure(engine.connect())
    current_rev = context.get_current_revision()

    if current_rev is None and not database_exists:
        META.create_all(engine)
    elif current_rev is None:
        inspected_meta = MetaData(bind=engine, reflect=True)
        # Inspect revisions for one or two releases, then bail if current_rev
        # is still None, asking to install an older release first.
        inspected_rev = 1
        upgrade_with_range(context, inspected_rev, REV)
    else:
        current_rev = int(current_rev)
        upgrade_with_range(context, current_rev, REV)
    context._update_current_rev(current_rev, REV)

