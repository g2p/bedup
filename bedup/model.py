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

from sqlalchemy.orm import relationship, column_property
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_, select, func, literal_column, distinct
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.types import (
    Boolean, Integer, Text, DateTime, TypeDecorator)
from sqlalchemy.schema import (
    Column, ForeignKey, UniqueConstraint, CheckConstraint)

from zlib import adler32
from . import fiemap
from .datetime import UTC


def FK(cattr, primary_key=False, backref=None, nullable=False):
    col, = cattr.property.columns
    return (
        Column(
            col.type, ForeignKey(col),
            primary_key=primary_key,
            nullable=nullable),
        relationship(cattr.parententity, backref=backref))


class UTCDateTime(TypeDecorator):
    impl = DateTime

    def process_bind_param(self, value, engine):
        return value.astimezone(UTC)

    def process_result_value(self, value, engine):
        return value.replace(tzinfo=UTC)


# XXX I actually need create_or_update here
def get_or_create(sess, model, **kwargs):
    try:
        return sess.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        instance = model(**kwargs)
        # XXX Some of the relationship attributes remain unset at this point
        sess.add(instance)
        return instance, True


class SuperBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
Base = declarative_base(cls=SuperBase)


class Filesystem(Base):
    id = Column(Integer, primary_key=True)
    uuid = Column(
        Text, CheckConstraint("uuid != ''"),
        unique=True, index=True, nullable=False)
    __table_args__ = (
        dict(
            sqlite_autoincrement=True))


class Volume(Base):
    # SmallInteger might be preferrable here,
    # but would require reimplementing an autoincrement
    # sequence outside of sqlite
    id = Column(Integer, primary_key=True)
    fs_id, fs = FK(Filesystem.id, backref='volumes')
    __table_args__ = (
        UniqueConstraint(
            'fs_id', 'root_id'),
        dict(
            sqlite_autoincrement=True))
    root_id = Column(Integer, nullable=False)
    last_tracked_generation = Column(Integer, nullable=False, default=0)
    last_tracked_size_cutoff = Column(Integer, nullable=True)
    size_cutoff = Column(Integer, nullable=False)


class VolumePathHistory(Base):
    id = Column(Integer, primary_key=True)
    vol_id, vol = FK(Volume.id, backref='path_history')
    # Paths in the / filesystem.
    # Paths relative to the root volume is harder (see volumes_from_root_tree).
    path = Column(Text, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'vol_id', 'path'),
        dict(
            sqlite_autoincrement=True))


class Inode(Base):
    vol_id, vol = FK(Volume.id, primary_key=True)
    # inode number
    ino = Column(Integer, primary_key=True)
    # We learn the size at the same time as the inode number,
    # and it's the first criterion we'll use, so not nullable
    size = Column(Integer, index=True, nullable=False)
    mini_hash = Column(Integer, index=True, nullable=True)
    # A digest of that file's FIEMAP extent info.
    fiemap_hash = Column(Integer, index=True, nullable=True)

    # has_updates gets set whenever this inode
    # appears in the volume scan, and reset whenever we do
    # a dedup pass.
    has_updates = Column(Boolean, index=True, nullable=False)

    fs_id = column_property(
        select([Volume.fs_id]).where(Volume.id == vol_id).label('fs_id'))

    def mini_hash_from_file(self, rfile):
        # A very cheap, very partial hash for quick disambiguation
        # Won't help with things like zeroed or sparse files.
        # The mini_hash for those is 0x10000001
        rfile.seek(int(self.size * .3))
        # bitops to make unsigned, for better readability
        self.mini_hash = adler32(rfile.read(4096)) & 0xffffffff

    def fiemap_hash_from_file(self, rfile):
        extents = tuple(fiemap.fiemap(rfile.fileno()))
        self.fiemap_hash = hash(extents)

    def __repr__(self):
        return 'Inode(ino=%d, volume=%d)' % (self.ino, self.vol_id)


# The logging classes don't have anything in common (no FKs)
# with the tracking classes. For example, inode numbers may
# be reused, and inodes can be removed from tracking in these
# cases. That would cause dangling references or delete cascades.
# We do allow FKs to volumes; those aren't meant to be removed.
class DedupEvent(Base):
    id = Column(Integer, primary_key=True)
    fs_id, fs = FK(Filesystem.id)

    item_size = Column(Integer, index=True, nullable=False)
    created = Column(UTCDateTime, index=True, nullable=False)

    @hybrid_property
    def estimated_space_gain(self):
        return self.item_size * (self.inode_count - 1)

    __table_args__ = (
        dict(
            sqlite_autoincrement=True))


class DedupEventInode(Base):
    id = Column(Integer, primary_key=True)
    event_id, event = FK(DedupEvent.id)
    ino = Column(Integer, index=True, nullable=False)
    vol_id, vol = FK(Volume.id)

    __table_args__ = (
        dict(
            sqlite_autoincrement=True))

DedupEvent.inode_count = column_property(
    select([func.count(DedupEventInode)])
    .where(DedupEventInode.vol_id == Volume.id)
    .label('inode_count'))


def comm_mappings(vol_ids):
    # XXX Is there a way to factor the vol_id.in_ as a subquery?
    # or to have a Comm3 -> Comm2 -> Comm1 relationship?

    class Commonality1(Base):
        __table__ = select([
            Inode.fs_id,
            Inode.size,
            func.count().label('inode_count'),
            func.max(Inode.has_updates).label('has_updates'),
        ]).where(
            Inode.vol_id.in_(vol_ids)
        ).group_by(
            Inode.fs_id,
            Inode.size,
        ).having(and_(
            literal_column('inode_count') > 1,
            literal_column('has_updates') > 0,
        )).alias()

        __mapper_args__ = (
            dict(primary_key=[
                __table__.c.fs_id,
                __table__.c.size,
            ]))

        inodes = relationship(
            Inode,
            primaryjoin=and_(
                Inode.fs_id == __table__.c.fs_id,
                Inode.vol_id.in_(vol_ids),
                Inode.size == __table__.c.size),
            foreign_keys=list(Inode.__table__.c))

    class Commonality2(Base):
        __table__ = select([
            Inode.fs_id,
            Inode.size,
            Inode.mini_hash,
            func.count().label('inode_count'),
            func.max(Inode.has_updates).label('has_updates'),
        ]).where(and_(
            Inode.mini_hash != None,
            Inode.vol_id.in_(vol_ids),
        )).group_by(
            Inode.fs_id,
            Inode.size,
            Inode.mini_hash,
        ).having(and_(
            literal_column('inode_count') > 1,
            literal_column('has_updates') > 0,
        )).alias()

        __mapper_args__ = (
            dict(primary_key=[
                __table__.c.fs_id,
                __table__.c.size,
                __table__.c.mini_hash,
            ]))

        inodes = relationship(
            Inode,
            primaryjoin=and_(
                Inode.fs_id == __table__.c.fs_id,
                Inode.vol_id.in_(vol_ids),
                Inode.size == __table__.c.size,
                Inode.mini_hash == __table__.c.mini_hash),
            foreign_keys=list(Inode.__table__.c))

    class Commonality3(Base):
        __table__ = select([
            Inode.fs_id,
            Inode.size,
            Inode.mini_hash,
            func.count(distinct(Inode.fiemap_hash)).label('fiemap_count'),
            func.max(Inode.has_updates).label('has_updates'),
        ]).where(and_(
            Inode.mini_hash != None,
            Inode.fiemap_hash != None,
            Inode.vol_id.in_(vol_ids),
        )).group_by(
            Inode.fs_id,
            Inode.size,
            Inode.mini_hash,
        ).having(and_(
            literal_column('fiemap_count') > 1,
            literal_column('has_updates') > 0,
        )).alias()

        __mapper_args__ = (
            dict(primary_key=[
                __table__.c.fs_id,
                __table__.c.size,
                __table__.c.mini_hash,
            ]))

        inodes = relationship(
            Inode,
            primaryjoin=and_(
                Inode.fs_id == __table__.c.fs_id,
                Inode.vol_id.in_(vol_ids),
                Inode.size == __table__.c.size,
                Inode.mini_hash == __table__.c.mini_hash),
            foreign_keys=list(Inode.__table__.c))

    # Commonality4 would be a crypto hash, but it's not part of this model atm
    return Commonality1, Commonality2, Commonality3

META = Base.metadata

