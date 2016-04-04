#
# Copyright 2016 Universidad Complutense de Madrid
#
# This file is part of Numina
#
# Numina is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Numina is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Numina.  If not, see <http://www.gnu.org/licenses/>.
#

"""User command line interface of Numina."""

import datetime

from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import UniqueConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, CheckConstraint, desc
from sqlalchemy import Integer, String, DateTime, Float, Boolean, TIMESTAMP
from sqlalchemy import Table, Column, ForeignKey
# from sqlalchemy import PickleType, Enum
from sqlalchemy.orm import relationship, backref, synonym
# from sqlalchemy.orm.collections import attribute_mapped_collection
# from sqlalchemy.orm import validates


import numina.core.dataframe


Base = declarative_base()


class MyOb(Base):
    __tablename__ = 'obs'

    id = Column(Integer, primary_key=True)
    instrument = Column(String, nullable=False)
    mode = Column(String, nullable=False)
    start_time = Column(DateTime, default=datetime.datetime.utcnow)
    completion_time = Column(DateTime)
    frames = relationship("Frame", back_populates='ob')


class Frame(Base):
    __tablename__ = 'frames'
    id = Column(Integer, primary_key=True)
    name = Column(String(10), unique=True, nullable=False)
    ob_id = Column(Integer,  ForeignKey("obs.id"), nullable=False)
    ob = relationship("MyOb", back_populates='frames')
    #
    filename = synonym("name")

    def to_numina_frame(self):
        return numina.core.dataframe.DataFrame(filename=self.filename)


class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    ob_id = Column(Integer,  ForeignKey("obs.id"), nullable=False)
    ob = relationship("MyOb")
    start_time = Column(DateTime, default=datetime.datetime.utcnow)
    completion_time = Column(DateTime)


class DataProduct(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    instrument_id = Column(String(10))
    datatype = Column(String(45))
    task_id = Column(Integer, ForeignKey('tasks.id'))
    contents = Column(String(45))

