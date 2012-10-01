
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.types import (UnicodeText, Integer, SmallInteger, Binary)
from sqlalchemy.util import generic_repr
from sqlalchemy import (
    Column, ForeignKey, UniqueConstraint)
from zlib import adler32


def FK(cattr, primary_key=False, backref=None, nullable=False):
    col, = cattr.property.columns
    return (
        Column(
            col.type, ForeignKey(col),
            primary_key=primary_key,
            nullable=nullable),
        relationship(cattr.parententity, backref=backref))


def FK2(entity, primary_key=False, backref=None, nullable=False):
    # We could also return a composite column rather than multiple columns,
    # but that may require a dynamically built class for the column type.
    def gen_fk_cols():
        for col in entity.__table__.primary_key:
            yield Column(
                col.type, ForeignKey(col),
                primary_key=primary_key,
                nullable=nullable)
    fk_cols = list(gen_fk_cols())
    join_conditions = list(
        col1==col2
        for (col1, col2) in zip(entity.__table__.primary_key, fk_cols))
    rel = relationship(
        entity, backref=backref, uselist=False,
        primaryjoin=and_(*join_conditions),
    )
    return fk_cols, rel


# XXX I actually need create_or_update here
def get_or_create(sess, model, **kwargs):
    try:
        return sess.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        instance = model(**kwargs)
        sess.add(instance)
        return instance, True


class SuperBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
Base = declarative_base(cls=SuperBase)


class Filesystem(Base):
    id = Column(SmallInteger, primary_key=True)
    __table_args__ = dict(sqlite_autoincrement=True)
    uuid = Column(Binary(16), unique=True, nullable=False)
    last_tracked_generation = Column(Integer, nullable=False)


class InodeAndSize(Base):
    fs_id, fs = FK(Filesystem.id, primary_key=True)
    inode = Column(Integer, primary_key=True)
    size = Column(Integer, index=True, nullable=False)

    def __repr__(self):
        return 'InodeAndSize(inode=%r, size=%r)' % (self.inode, self.size)


class MiniHash(Base):
    (fs_id, inode), ias = FK2(InodeAndSize, primary_key=True)
    mini_hash = Column(Integer, nullable=False)

    def update_from_file(self, rfile):
        # A very cheap, very partial hash for quick disambiguation
        # Won't help with things like zeroed or sparse files.
        # The mini_hash for those is 0x10000001
        rfile.seek(int(self.ias.size * .3))
        self.mini_hash = adler32(rfile.read(4096))


META = Base.metadata

