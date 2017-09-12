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

import os
import json

from numina.util.context import working_directory

import astropy.io.fits as fits
import numina.drps

from .model import MyOb, Frame, Fact

def metadata_fits(fname):
    result = {}
    with fits.open(fname) as hdulist:
        keys = ['DATE-OBS', 'VPH', 'INSMODE',
                'obsmode', 'insconf', 'blckuuid',
                'instrume', 'uuid', 'numtype']
        for key in keys:
            result[key] = hdulist[0].header.get(key)

    return result


def metadata_json(fname):
    result = {}
    with open(fname) as fd:
        data = json.load(fd)
        result['tags'] = data['tags']
        result['instrument'] = data['instrument']
        result['numtype'] = data['type']
        result['uuid'] = data['uuid']
    return result


def add_product_facts(session, prod, datadir):

    drps = numina.drps.get_system_drps()

    this_drp = drps.query_by_name(prod.instrument_id)
    pipeline = this_drp.pipelines['default']

    prodtype = pipeline.load_product_from_name(prod.datatype)

    with working_directory(datadir):
        master_tags = prodtype.extract_tags(prod.contents)

    for k, v in master_tags.items():
        fact = session.query(Fact).filter_by(key=k, value=v).first()
        if fact is None:
            fact = Fact(key=k, value=v)
        prod.facts.append(fact)


def add_ob_facts(session, ob, datadir):

    drps = numina.drps.get_system_drps()
    this_drp = drps.query_by_name(ob.instrument)

    tagger = None
    for mode in this_drp.modes:
        if mode.key == ob.mode:
            tagger = mode.tagger
            break
    if tagger:
        current = os.getcwd()
        os.chdir(datadir)
        master_tags = tagger(ob)
        os.chdir(current)
        print('master_tags', master_tags)
        for k, v in master_tags.items():
            fact = session.query(Fact).filter_by(key=k, value=v).first()
            if fact is None:
                fact = Fact(key=k, value=v)
            ob.facts.append(fact)