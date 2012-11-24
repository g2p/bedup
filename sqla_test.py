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


class Thing(Base):
    id = Column(Integer, primary_key=True)
    size = Column(Integer, nullable=False)


if False:
    # less likely to break?
    thing_size = Thing.__table__.c.size
else:
    thing_size = Thing.size


class Commonality(Base):
    __table__ = select([
        thing_size,
        #func.count().label('thing_count'),
    ]).group_by(thing_size).alias('comm')

    __mapper_args__ = (
        dict(primary_key=[
            __table__.c.size,
        ]))

    things = relationship(
        Thing,
        primaryjoin=thing_size == __table__.c.size,
        foreign_keys=[thing_size])


def main():
    engine = create_engine('sqlite:///:memory:', echo=True)
    Base.metadata.create_all(engine)
    sess = Session(bind=engine)
    thing = Thing(size=12)
    sess.add(thing)
    comm = sess.query(Commonality)[0]
    print(comm.things)


if __name__ == '__main__':
    sys.exit(main())

