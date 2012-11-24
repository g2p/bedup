#!/usr/bin/python3

import sys

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import *


class SuperBase:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
Base = declarative_base(cls=SuperBase)


def FK(cattr, primary_key=False, backref=None, nullable=False):
    col, = cattr.property.columns
    #import IPython; IPython.embed()
    # parententity was renamed to parent in 8863:8639ba1a9454
    return (
        Column(
            col.type, ForeignKey(col),
            primary_key=primary_key,
            nullable=nullable),
        relationship(cattr.parent, backref=backref))


class OuterContainer(Base):
    id = Column(Integer, primary_key=True)


class InnerContainer(Base):
    id = Column(Integer, primary_key=True)
    outer_id, outer = FK(OuterContainer.id)


class Thing(Base):
    id = Column(Integer, primary_key=True)
    size = Column(Integer, nullable=False)
    inner_id, inner = FK(InnerContainer.id)
    outer_id = column_property(
        select([InnerContainer.outer_id]).where(
            InnerContainer.id == inner_id).label('outer_id'))


if False:
    # less likely to break?
    thing_size = Thing.__table__.c.size
    #thing_outer_id = Thing.__table__.c.outer_id
    thing_outer_id = Thing.outer_id
else:
    thing_size = Thing.size
    thing_outer_id = Thing.outer_id


class Commonality(Base):
    __table__ = select([
        thing_size,
        thing_outer_id,
    ]).group_by(
        thing_size,
        thing_outer_id,
    ).alias('comm')

    __mapper_args__ = (
        dict(primary_key=[
            __table__.c.size,
            __table__.c.outer_id,
        ]))

    things = relationship(
        Thing,
        primaryjoin=thing_size == __table__.c.size,
        foreign_keys=[thing_size])


def main():
    engine = create_engine('sqlite:///:memory:', echo=True)
    Base.metadata.create_all(engine)
    sess = Session(bind=engine)
    outer = OuterContainer()
    inner = InnerContainer(outer=outer)
    thing = Thing(size=12, inner=inner)
    sess.add(thing)
    comm = sess.query(Commonality)[0]
    print(comm.things)


if __name__ == '__main__':
    sys.exit(main())

