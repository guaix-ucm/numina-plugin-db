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

db_info_keys = [
    'instrument',
    'object',
    'observation_date',
    'uuid',
    'type',
    'mode',
    'exptime',
    'darktime',
#    'insconf',
#    'blckuuid',
    'quality_control'
]

def metadata_fits(obj, drps):

    # First. get instrument
    objl = DataFrameType().convert(obj)

    with objl.open() as hdulist:
        # get instrument
        instrument_id = hdulist[0].header['INSTRUME']

    this_drp = drps.query_by_name(instrument_id)

    datamodel = this_drp.datamodel
    result = DataFrameType(datamodel=datamodel).extract_db_info(obj, db_info_keys)
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


def ingest_ob_file(session, path):
    import yaml
    import uuid
    import datetime
    import os.path

    from numina.core.oresult import ObservationResult
    from .model import ObservingBlockAlias

    drps = numina.drps.get_system_drps()

    print("mode ingest, ob file, path=", path)

    obs_blocks = {}
    with open(path, 'r') as fd:
        loaded_data = yaml.load_all(fd)

        # complete the blocks...
        for el in loaded_data:
            obs_blocks[el['id']] = el

    # FIXME: id could be UUID
    obs_blocks1 = {}
    for ob_id, block in obs_blocks.items():
        ob = ObservationResult(
            instrument=block['instrument'],
            mode=block['mode']
        )
        ob.id = ob_id
        ob.uuid = str(uuid.uuid4())
        ob.configuration = 'default'
        ob.children = block.get('children', [])
        ob.frames = block.get('frames', [])
        obs_blocks1[ob_id] = ob
        # ignore frames for the moment

    obs_blocks2 = {}
    for key, obs in obs_blocks1.items():
        now = datetime.datetime.now()
        ob = MyOb(instrument_id=obs.instrument, mode=obs.mode, start_time=now)
        ob.id = obs.uuid
        obs_blocks2[obs.id] = ob
        # FIXME: add alias, only if needed
        alias = ObservingBlockAlias(uuid=obs.uuid, alias=obs.id)

        # add frames
        # extract metadata from frames
        # FIXME:
        ingestdir = 'data'
        meta_frames = []
        for fname in obs.frames:
            full_fname = os.path.join(ingestdir, fname)
            result = metadata_fits(full_fname, drps)
            #numtype = result['type']
            #blck_uuid = obs.uuid # result.get('blckuuid', obs.uuid)
            result['path'] = fname
            meta_frames.append(result)

        for meta in meta_frames:
            # Insert into DB
            newframe = Frame()
            newframe.name = meta['path']
            newframe.uuid = meta['uuid']
            newframe.start_time = meta['observation_date']
            # No way of knowing when the readout ends...
            newframe.completion_time = newframe.start_time + datetime.timedelta(seconds=meta['darktime'])
            newframe.exposure_time = meta['exptime']
            newframe.object = meta['object']
            ob.frames.append(newframe)

        # set start/completion time from frames
        if ob.frames:
            ob.object = meta_frames[0]['object']
            ob.start_time = ob.frames[0].start_time
            ob.completion_time = ob.frames[-1].completion_time

        # Facts
        #add_ob_facts(session, ob, ingestdir)

        # raw frames insertion
        # for frame in frames:
        # call_event('on_ingest_raw_fits', session, frame, frames[frame])

        session.add(ob)
        session.add(alias)

    # processes children
    for key, obs in obs_blocks1.items():
        if obs.children:
            # get parent
            parent = obs_blocks2[key]
            for cid in obs.children:
                # get children
                child = obs_blocks2[cid]
                parent.children.append(child)

    for key, obs in obs_blocks2.items():
        if obs.object is None:
            o1, s1, c1 = complete_recursive_first(obs)
            o2, s2, c2 = complete_recursive_last(obs)
            obs.object = o1
            obs.start_time = s1
            obs.completion_time = c2

    session.commit()


def complete_recursive_first(node):
    return complete_recursive_idx(node, 0)


def complete_recursive_last(node):
    return complete_recursive_idx(node, -1)


def complete_recursive_idx(node, idx):
    if node.object is None:
        if node.children:
            value = complete_recursive_idx(node.children[idx], idx)
            if value is not None:
                return value
        else:
            return None
    else:
        return (node.object, node.start_time, node.completion_time)
