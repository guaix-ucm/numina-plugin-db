#
# Copyright 2016-2017 Universidad Complutense de Madrid
#
# This file is part of Numina DB
#
# SPDX-License-Identifier: GPL-3.0+
# License-Filename: LICENSE.txt
#

"""Model for SQL tables."""

import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, UnicodeText
from sqlalchemy import CHAR
from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy import Enum
from sqlalchemy.orm import relationship, backref, synonym
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.ext.associationproxy import association_proxy
import numina.types.dataframe
import numina.types.qc as qc

from numinadb.base import Base
from .jsonsqlite import MagicJSON
from .polydict import PolymorphicVerticalProperty
from .proxydict import ProxiedDictMixin


class Instrument(Base):
    __tablename__ = 'instruments'
    name = Column(String(10), primary_key=True)


class ObservingBlock(Base):
    __tablename__ = 'obs'

    id = Column(String, primary_key=True)
    instrument_id = Column(String(10), ForeignKey("instruments.name"), nullable=False)
    mode = Column(String, nullable=False)
    object = Column(String)
    parent_id = Column(String, ForeignKey('obs.id'))
    start_time = Column(DateTime)
    completion_time = Column(DateTime)

    frames = relationship("Frame", back_populates='ob')
    facts = relationship('Fact', secondary='data_obs_fact')
    instrument = synonym("instrument_id")
    instrument_ = relationship("Instrument")

    children = relationship(
        "ObservingBlock",
        backref=backref('parent', remote_side=[id])
    )
    images = synonym("frames")
    # Compatibility
    requirements = []
    results = []

    def metadata_with(self, datamodel):
        origin = {}
        imginfo = datamodel.gather_info_oresult(self)
        origin['info'] = imginfo
        first = imginfo[0]
        origin["block_uuid"] = first['block_uuid']
        origin['insconf_uuid'] = first['insconf_uuid']
        origin['date_obs'] = first['observation_date']
        origin['observation_date'] = first['observation_date']
        origin['frames'] = [img['imgid'] for img in imginfo]
        return origin


class ObservingBlockAlias(Base):
    __tablename__ = 'obs_alias'

    id = Column(Integer, primary_key=True)
    uuid = Column(String, ForeignKey("obs.id"), nullable=False)
    alias = Column(String, nullable=False, unique=True)


class Fact(Base):
    """A fact about an OB."""

    __tablename__ = 'fact'

    id = Column(Integer, primary_key=True)
    key = Column(String(64))
    value = Column(String(64))


class ProductFact(PolymorphicVerticalProperty, Base):
    """A fact about an OB."""

    __tablename__ = 'product_facts'
    owner_id = Column(ForeignKey('products.id'), primary_key=True)
    key = Column(String, primary_key=True)
    type = Column(String(16))

    # add information about storage for different types
    # in the info dictionary of Columns
    int_value = Column(Integer, info={'type': (int, 'integer')})
    char_value = Column(String, info={'type': (str, 'string')})
    # unicode_value = Column(String, info={'type': (unicode, 'unicode')})
    boolean_value = Column(Boolean, info={'type': (bool, 'boolean')})
    float_value = Column(Float, info={'type': (float, 'float')})


class ParameterFact(PolymorphicVerticalProperty, Base):
    """A fact about an OB."""

    __tablename__ = 'parameter_facts'
    owner_id = Column(ForeignKey('recipe_parameter_values.id'), primary_key=True)
    key = Column(String(64), primary_key=True)
    type = Column(String(16))

    # add information about storage for different types
    # in the info dictionary of Columns
    int_value = Column(Integer, info={'type': (int, 'integer')})
    char_value = Column(String, info={'type': (str, 'string')})
    boolean_value = Column(Boolean, info={'type': (bool, 'boolean')})
    float_value = Column(Float, info={'type': (float, 'float')})


class Frame(Base):
    __tablename__ = 'frames'
    id = Column(Integer, primary_key=True)
    uuid = Column(CHAR(32), nullable=True)
    name = Column(String(100), unique=True, nullable=False)
    ob_id = Column(String,  ForeignKey("obs.id"), nullable=False)
    object = Column(String)
    start_time = Column(DateTime)
    exposure_time = Column(Float)
    completion_time = Column(DateTime)
    ob = relationship("ObservingBlock", back_populates='frames')
    #
    filename = synonym("name")

    def open(self):
        from astropy.io import fits
        return fits.open(self.name, mode='readonly')

    def to_numina_frame(self):
        return numina.types.dataframe.DataFrame(filename=self.filename)


class DataProcessingTask(Base):
    __tablename__ = 'dp_task'
    id = Column(Integer, primary_key=True)
    host = Column(String(45))
    state = Column(Integer, default=0)
    create_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    start_time = Column(DateTime)
    completion_time = Column(DateTime)

    # obsresult_node_id = Column(Integer, ForeignKey('observation_result.id'), nullable=False)
    ob_id = Column(String, ForeignKey("obs.id"), nullable=False)

    parent_id = Column(Integer, ForeignKey('dp_task.id'))
    label = Column(String(255))
    waiting = Column(Boolean)
    awaited = Column(Boolean)
    method = Column(UnicodeText)
    request = Column(MagicJSON)
    result = Column(MagicJSON)

    # obsresult_node = relationship("ObservationResult", backref='tasks')
    ob = relationship("ObservingBlock", backref='tasks')

    children = relationship("DataProcessingTask", backref=backref('parent', remote_side=[id]))


class ReductionResult(Base):
    __tablename__ = 'reduction_results'
    id = Column(Integer, primary_key=True)
    instrument_id = Column(String(10), ForeignKey("instruments.name"), nullable=False)

    pipeline = Column(String(20))
    obsmode = Column(String(40))
    recipe = Column(String(100))

    task_id = Column(Integer, ForeignKey('dp_task.id'), unique=True, nullable=False)
    ob_id = Column(String, ForeignKey("obs.id"), nullable=False)
    qc = Column(Enum(qc.QC), default=qc.QC.UNKNOWN)

    values = relationship("ReductionResultValue")
    task = relationship("DataProcessingTask", backref=backref('reduction_result', uselist=False))
    ob = relationship("ObservingBlock", backref=backref('reduction_results'))


class ReductionResultValue(Base):
    __tablename__ = 'reduction_result_values'
    id = Column(Integer, primary_key=True)
    result_id = Column(Integer, ForeignKey('reduction_results.id'), nullable=False)
    result = relationship("ReductionResult")
    name = Column(String(45))
    datatype = Column(String(45))
    contents = Column(String(45))


class DataProduct(ProxiedDictMixin, Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    instrument_id = Column(String(10), ForeignKey("instruments.name"), nullable=False)
    datatype = Column(String(45))
    task_id = Column(Integer, ForeignKey('dp_task.id'))
    result_id = Column(Integer, ForeignKey('reduction_result_values.id'))
    uuid = Column(CHAR(32))
    dateobs = Column(DateTime)
    qc = Column(Enum(qc.QC), default=qc.QC.UNKNOWN)
    priority = Column(Integer, default=0)
    contents = Column(String(45))

    result_value = relationship("ReductionResultValue")

    facts = relationship("ProductFact", collection_class=attribute_mapped_collection('key'))

    crel = lambda key, value: ProductFact(key=key, value=value)  # noqa
    _proxied = association_proxy("facts", "value", creator=crel)

    def __init__(self, instrument_id, datatype, task_id, contents, priority=0):
        self.instrument_id = instrument_id
        self.datatype = datatype
        self.task_id = task_id
        self.contents = contents
        self.priority = priority

    @classmethod
    def with_characteristic(cls, key, value):
        return cls.facts.any(key=key, value=value)


data_obs_fact = Table(
    'data_obs_fact', Base.metadata,
    Column('obs_id', Integer, ForeignKey('obs.id'), primary_key=True),
    Column('fact_id', Integer, ForeignKey('fact.id'), primary_key=True)
)


class RecipeParameters(Base):
    __tablename__ = 'recipe_parameters'
    __table_args__ = (UniqueConstraint('instrument_id', 'pipeline', 'mode', 'name'), )

    id = Column(Integer, primary_key=True)
    instrument_id = Column(String(10), ForeignKey("instruments.name"), nullable=False)
    pipeline = Column(String, default='default', nullable=False)
    mode = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    values = relationship("RecipeParameterValues", back_populates='parameter')


class RecipeParameterValues(ProxiedDictMixin, Base):
    __tablename__ = 'recipe_parameter_values'

    id = Column(Integer, primary_key=True)
    param_id = Column(String,  ForeignKey("recipe_parameters.id"), nullable=False)

    content = Column(MagicJSON, nullable=False)
    parameter = relationship("RecipeParameters")

    facts = relationship("ParameterFact", collection_class=attribute_mapped_collection('key'))

    crel = lambda key, value: ParameterFact(key=key, value=value)  # noqa
    _proxied = association_proxy("facts", "value", creator=crel)

    @classmethod
    def with_characteristic(cls, key, value):
        return cls.facts.any(key=key, value=value)
