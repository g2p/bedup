
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.util import generic_repr
from sqlalchemy import (
    UnicodeText, Integer, Binary, Column, ForeignKey, UniqueConstraint)


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
        sess.add(instance)
        return instance, True


class SuperBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
Base = declarative_base(cls=SuperBase)


class Filesystem(Base):
    id = Column(Integer, primary_key=True)
    __table_args__ = dict(sqlite_autoincrement=True)
    uuid = Column(Binary(16), unique=True)


class InodeAndSize(Base):
    fs_id, fs = FK(Filesystem.id, primary_key=True)
    inode = Column(Integer, primary_key=True)
    size = Column(Integer, index=True)

    def __repr__(self):
        return 'InodeAndSize(inode=%r, size=%r)' % (self.inode, self.size)

META = Base.metadata

