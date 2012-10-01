
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.util import generic_repr
from sqlalchemy import (
    UnicodeText, Integer, Column, ForeignKey, UniqueConstraint)


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


class InodeAndSize(Base):
    inode = Column(Integer, primary_key=True)
    size = Column(Integer, index=True)

    def __repr__(self):
        return 'InodeAndSize(inode=%r, size=%r)' % (self.inode, self.size)

META = Base.metadata

