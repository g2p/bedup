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

from sqlalchemy.orm import relationship, column_property, backref as backref_
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import select, func
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.types import (
    Boolean, Integer, Text, DateTime, TypeDecorator)
from sqlalchemy.schema import (
    Column, ForeignKey, UniqueConstraint, CheckConstraint)

from .datetime import UTC
from .hashing import mini_hash_from_file, fiemap_hash_from_file


def parent_entity(cattr):
    # This got renamed in 0.8, leaving no easy way to handle both versions.
    try:
        return cattr.parent
    except AttributeError:
        return cattr.parententity


def FK(cattr, primary_key=False, backref=None, nullable=False, cascade=False):
    # cascade=False will select the sqla default (save-update, merge)
    col, = cattr.property.columns
    if backref is None:
        assert cascade is False
    else:
        backref = backref_(backref, cascade=cascade)

    return (
        Column(
            col.type, ForeignKey(col),
            primary_key=primary_key,
            nullable=nullable),
        relationship(
            parent_entity(cattr), backref=backref))


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


class BtrfsFilesystem(Base):
    id = Column(Integer, primary_key=True)
    uuid = Column(
        Text, CheckConstraint("uuid != ''"),
        unique=True, index=True, nullable=False)
    __tablename__ = 'Filesystem'
    __table_args__ = (
        dict(
            sqlite_autoincrement=True))


class Volume(Base):
    # SmallInteger might be preferrable here,
    # but would require reimplementing an autoincrement
    # sequence outside of sqlite
    id = Column(Integer, primary_key=True)
    fs_id, fs = FK(BtrfsFilesystem.id, backref='volumes')
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
    vol_id, vol = FK(
        Volume.id, backref='path_history', cascade='all, delete-orphan')
    # Paths in the / filesystem.
    # For paths relative to the root volume, see read_root_tree
    path = Column(Text, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'vol_id', 'path'),
        dict(
            sqlite_autoincrement=True))


Volume.last_known_mountpoint = column_property(
    select([VolumePathHistory.path])
        .where(
            VolumePathHistory.vol_id == Volume.id)
        .order_by(-VolumePathHistory.id)
        .label('last_known_mountpoint'),
    deferred=True)


class InodeProps(object):
    @declared_attr
    def fs_id(cls):
        return column_property(
            select([Volume.fs_id]).where(
                Volume.id == cls.vol_id).label('fs_id'), deferred=True)

    def mini_hash_from_file(self, rfile):
        self.mini_hash = mini_hash_from_file(self, rfile)

    def fiemap_hash_from_file(self, rfile):
        self.fiemap_hash = fiemap_hash_from_file(rfile)


class Inode(Base, InodeProps):
    vol_id, vol = FK(
        Volume.id, primary_key=True, backref='inodes',
        cascade='all, delete-orphan')
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

    def __repr__(self):
        return 'Inode(ino=%d, volume=%d)' % (self.ino, self.vol_id)


Volume.inode_count = column_property(
    select([func.count(Inode.ino)])
        .where(Inode.vol_id == Volume.id)
        .label('inode_count'),
    deferred=True)


# The logging classes don't have anything in common (no FKs)
# with the tracking classes. For example, inode numbers may
# be reused, and inodes can be removed from tracking in these
# cases. That would cause dangling references or delete cascades.
# We do allow FKs to volumes; those aren't meant to be removed.
class DedupEvent(Base):
    id = Column(Integer, primary_key=True)
    fs_id, fs = FK(
        BtrfsFilesystem.id,
        backref='dedup_events', cascade='all, delete-orphan')

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
    event_id, event = FK(
        DedupEvent.id, backref='inodes', cascade='all, delete-orphan')
    ino = Column(Integer, index=True, nullable=False)
    vol_id, vol = FK(
        Volume.id, backref='dedup_event_inodes', cascade='all, delete-orphan')

    __table_args__ = (
        dict(
            sqlite_autoincrement=True))

DedupEvent.inode_count = column_property(
    select([func.count(DedupEventInode.id)])
    .where(DedupEventInode.event_id == DedupEvent.id)
    .label('inode_count'))


META = Base.metadata

