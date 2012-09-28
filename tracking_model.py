
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import (
    UnicodeText, Integer, Column, ForeignKey, UniqueConstraint)

class SuperBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
Base = declarative_base(cls=SuperBase)

class InodeAndSize(Base):
    inode = Column(Integer, primary_key=True)
    size = Column(Integer, index=True)

META = Base.metadata

