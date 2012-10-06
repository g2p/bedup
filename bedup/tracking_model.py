
from sqlalchemy.orm import relationship, column_property
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_, select, func, literal_column
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.types import (Boolean, Integer, Text)
from sqlalchemy.schema import (
    Column, ForeignKey, UniqueConstraint, CheckConstraint)
from zlib import adler32


def FK(cattr, primary_key=False, backref=None, nullable=False):
    col, = cattr.property.columns
    return (
        Column(
            col.type, ForeignKey(col),
            primary_key=primary_key,
            nullable=nullable),
        relationship(cattr.parententity, backref=backref))


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
    last_tracked_generation = Column(Integer, nullable=False)


class Inode(Base):
    vol_id, vol = FK(Volume.id, primary_key=True)
    inode = Column(Integer, primary_key=True)
    # We learn the size at the same time as inode number,
    # and it's the first criterion we'll use, so not nullable
    size = Column(Integer, index=True, nullable=False)
    mini_hash = Column(Integer, index=True, nullable=True)

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

    def __repr__(self):
        return 'Inode(inode=%d, volume=%d)' % (self.inode, self.vol_id)


def comm_mappings(vol_ids):
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
        ]).where(
            Inode.vol_id.in_(vol_ids)
        ).group_by(
            Inode.fs_id,
            Inode.size,
            Inode.mini_hash,
        ).having(and_(
            Inode.mini_hash != None,
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

    # Commonality3 would be a crypto hash, but it's not part of this model atm
    return Commonality1, Commonality2

META = Base.metadata

