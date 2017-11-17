#
# Copyright 2017 Universidad Complutense de Madrid
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

"""Ingestion of different types."""

import json

from numina.types.frame import DataFrameType
from numina.util.convert import convert_date
from numina.types.linescatalog import LinesCatalog
from numina.types.structured import BaseStructuredCalibration
from numina.util.context import working_directory
import numina.drps

from .model import MyOb, Frame, Fact


def metadata_fits(obj, drps):

    # First. get instrument
    objl = DataFrameType().convert(obj)

    with objl.open() as hdulist:
        # get instrument
        instrument_id = hdulist[0].header['INSTRUME']

    this_drp = drps.query_by_name(instrument_id)

    datamodel = this_drp.datamodel
    result = DataFrameType(datamodel=datamodel).extract_db_info(obj)
    return result


def metadata_lis(obj):
    """Extract metadata from serialized file"""
    result = LinesCatalog().extract_db_info(obj)
    import os

    head, tail = os.path.split(obj)
    base, ext = os.path.splitext(tail)
    tags = base.split('_')
    result['instrument'] = 'MEGARA'
    result['tags'] = {
        u'vph': tags[0].decode('utf-8'),
        u'speclamp': tags[1].decode('utf-8')
    }

    return result


def metadata_json(obj):
    """Extract metadata from serialized file"""

    result = BaseStructuredCalibration().extract_meta_info(obj)
    return result

def _add_product_facts(session, prod, datadir):
    drps = numina.drps.get_system_drps()

    this_drp = drps.query_by_name(prod.instrument_id)
    pipeline = this_drp.pipelines['default']

    prodtype = pipeline.load_product_from_name(prod.datatype)

    # with working_directory(datadir):
    master_tags = prodtype.extract_tags(prod.contents)

    for k, v in master_tags.items():
        prod[k] = v


def add_ob_facts(session, ob, datadir):
    drps = numina.drps.get_system_drps()
    this_drp = drps.query_by_name(ob.instrument_id)

    tagger_func = None
    for mode in this_drp.modes:
        if mode.key == ob.mode:
            tagger_func = mode.tagger
            break
    if tagger_func:
        with working_directory(datadir):
            master_tags = tagger_func(ob)

        # print('master_tags', master_tags)
        for k, v in master_tags.items():
            fact = session.query(Fact).filter_by(key=k, value=v).first()
            if fact is None:
                fact = Fact(key=k, value=v)
            ob.facts.append(fact)
