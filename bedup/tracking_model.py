
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_, select, func, literal_column
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.types import (Boolean, Integer, Binary)
from sqlalchemy.schema import (Column, ForeignKey, UniqueConstraint)
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


# XXX Actually a subvolume
class Filesystem(Base):
    # SmallInteger might be preferrable here,
    # but would require reimplementing an autoincrement
    # sequence outside of sqlite
    id = Column(Integer, primary_key=True)
    __table_args__ = (
        UniqueConstraint(
            'uuid', 'root_id'),
        dict(
            sqlite_autoincrement=True))
    uuid = Column(Binary(16), nullable=False)
    root_id = Column(Integer, nullable=False)
    last_tracked_generation = Column(Integer, nullable=False)


class Inode(Base):
    fs_id, fs = FK(Filesystem.id, primary_key=True)
    inode = Column(Integer, primary_key=True)
    # We learn the size at the same time as inode number,
    # and it's the first criterion we'll use, so not nullable
    size = Column(Integer, index=True, nullable=False)
    mini_hash = Column(Integer, index=True, nullable=True)

    # has_updates gets set whenever this inode
    # appears in the fs scan, and reset whenever we do
    # a dedup pass.
    has_updates = Column(Boolean, index=True, nullable=False)

    def mini_hash_from_file(self, rfile):
        # A very cheap, very partial hash for quick disambiguation
        # Won't help with things like zeroed or sparse files.
        # The mini_hash for those is 0x10000001
        rfile.seek(int(self.size * .3))
        # bitops to make unsigned, for better readability
        self.mini_hash = adler32(rfile.read(4096)) & 0xffffffff

    def __repr__(self):
        return 'Inode(inode=%d, fs=%d)' % (self.inode, self.fs_id)


class Commonality1(Base):
    __table__ = select([
        Inode.fs_id,
        Inode.size,
        func.count().label('inode_count'),
        func.max(Inode.has_updates).label('has_updates'),
    ]).group_by(
        Inode.fs_id,
        Inode.size,
    ).having(and_(
        literal_column('inode_count') > 1,
        literal_column('has_updates') > 0,
    )).alias()

    fs_id = Inode.fs_id
    size = Inode.size

    inodes = relationship(
        Inode,
        primaryjoin=and_(
            __table__.c.fs_id == Inode.fs_id,
            __table__.c.size == Inode.size),
        foreign_keys=list(Inode.__table__.c))


class Commonality2(Base):
    __table__ = select([
        Inode.fs_id,
        Inode.size,
        Inode.mini_hash,
        func.count().label('inode_count'),
        func.max(Inode.has_updates).label('has_updates'),
    ]).group_by(
        Inode.fs_id,
        Inode.size,
        Inode.mini_hash,
    ).having(and_(
        Inode.mini_hash != None,
        literal_column('inode_count') > 1,
        literal_column('has_updates') > 0,
    )).alias()

    fs_id = Inode.fs_id
    size = Inode.size
    mini_hash = Inode.mini_hash

    inodes = relationship(
        Inode,
        primaryjoin=and_(
            __table__.c.fs_id == Inode.fs_id,
            __table__.c.size == Inode.size,
            __table__.c.mini_hash == Inode.mini_hash),
        foreign_keys=list(Inode.__table__.c))

# Commonality3 would be a crypto hash, but it's not part of this model atm

META = Base.metadata

