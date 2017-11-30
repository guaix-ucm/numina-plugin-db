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

from __future__ import print_function

import uuid
import datetime
import os.path

import yaml
from numina.core.oresult import ObservationResult
from numina.types.frame import DataFrameType
from numina.types.linescatalog import LinesCatalog
from numina.types.structured import BaseStructuredCalibration
from numina.util.context import working_directory
import numina.store
import numina.drps

from .model import RecipeParameters, RecipeParameterValues
from .model import ObservingBlockAlias
from .model import ObservingBlock, Frame, Fact, DataProduct
from .event import call_event


base_db_info_keys = [
    'instrument',
    'object',
    'observation_date',
    'uuid',
    'type',
    'mode',
    'exptime',
    'darktime',
    'insconf',
    'blckuuid',
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
    keys = datamodel.db_info_keys
    result = DataFrameType(datamodel=datamodel).extract_db_info(obj, keys)
    return result


def metadata_lis(obj):
    """Extract metadata from serialized file"""
    result = LinesCatalog().extract_db_info(obj, base_db_info_keys)

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


def ingest_control_file(session, path):

    print('insert task-control values from', path)

    with open(path) as fd:
        data = yaml.load(fd)

    res = data.get('requirements', {})

    for ins, data1 in res.items():
        for plp, modes in data1.items():
            for mode, params in modes.items():
                for param in params:
                    dbpar = session.query(RecipeParameters).filter_by(instrument_id=ins,
                                                                   pipeline=plp,
                                                                   mode=mode,
                                                                   name=param['name']).first()
                    if dbpar is None:
                        newpar = RecipeParameters()
                        newpar.id = None
                        newpar.instrument_id = ins
                        newpar.pipeline = plp
                        newpar.mode = mode
                        newpar.name = param['name']
                        dbpar = newpar
                        session.add(dbpar)

                    newval = RecipeParameterValues()
                    newval.content = param['content']
                    dbpar.values.append(newval)

                    for k, v in param['tags'].items():
                        newval[k] = v
    session.commit()


def ingest_ob_file(session, path):

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
        ob = ObservingBlock(instrument_id=obs.instrument, mode=obs.mode, start_time=now)
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
            print(fname, full_fname)
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

    print('stage4')
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


def ingest_dir(session, ingestdir):

    drps = numina.drps.get_system_drps()
    # insert OB in database

    print("mode ingest dir, path=", ingestdir)

    obs_blocks = {}
    raw_frames = {}
    reduction_results = {}

    for x in os.walk(ingestdir):
        dirname, dirnames, files = x
        # print('we are in', dirname)
        # print('dirnames are', dirnames)
        for fname in files:
            # check based on extension
            base, ext = os.path.splitext(fname)
            full_fname = os.path.join(dirname, fname)
            if ext == '.fits':
                # something
                print("file processed as FITS", full_fname)
                result = metadata_fits(full_fname, drps)

                # numtype
                numtype = result['type']
                blck_uuid = result.get('blckuuid')
                if numtype is not None:
                    # a calibration
                    print("a calibration of type", numtype)
                    reduction_uuid = result['uuid']
                    print("a calibration of type {}, uuid {}".format(numtype, reduction_uuid))
                    reduction_results[full_fname] = (numtype, full_fname, result, True)
                    continue
                else:
                    print('raw data')
                if blck_uuid is not None:
                    if blck_uuid not in obs_blocks:
                        print('added new OB', blck_uuid)
                        # new block, insert
                        ob = ObservationResult(
                            instrument=result['instrument'],
                            mode=result['mode']
                        )
                        ob.id = blck_uuid
                        ob.configuration = result['insconf']

                        obs_blocks[blck_uuid] = ob

                    uuid_frame = result['uuid']
                    if uuid_frame not in raw_frames:
                        result['path'] = fname
                        raw_frames[uuid_frame] = result
                        obs_blocks[blck_uuid].frames.append(result)

            elif ext == '.json':
                print("file ingested as JSON", fname)
                result = metadata_json(full_fname)
                numtype = result['type']
                print(full_fname, "a calibration of type", numtype)
                reduction_uuid = result['uuid']
                reduction_results[reduction_uuid] = (numtype, full_fname, result, False)
            elif ext == '.lis':
                result = metadata_lis(full_fname)
                numtype = result['type']
                print(full_fname, "a calibration of type", numtype)
                reduction_uuid = result['uuid']
                reduction_results[reduction_uuid] = (numtype, full_fname, result, False)
            else:
                print("file not ingested", fname)

    # insert OB in database
    print('processing reduction_results')
    for key, prod in reduction_results.items():

        datatype = prod[0]
        contents = prod[1]
        metadata_basic = prod[2]
        recheck = prod[3]
        fullpath = contents
        relpath = fullpath # os.path.relpath(fullpath, self.runinfo['base_dir'])
        print('processing', relpath)
        prod_entry = DataProduct(instrument_id=metadata_basic['instrument'],
                                 datatype=datatype,
                                 task_id=0,
                                 contents=relpath
                                 )

        if recheck:
            print('recheck metadata')
            this_drp = drps.query_by_name(prod_entry.instrument_id)
            pipeline = this_drp.pipelines['default']
            db_info_keys = this_drp.datamodel.db_info_keys
            prodtype = pipeline.load_product_from_name(prod_entry.datatype)
            # reread with correct type
            obj = numina.store.load(prodtype, prod_entry.contents)
            # extend metadata
            metadata_basic = prodtype.extract_db_info(obj, db_info_keys)

        # check if is already inserted
        res = session.query(DataProduct).filter_by(uuid=metadata_basic['uuid']).first()

        if res is not None:
            print('this product is already inserted', metadata_basic['uuid'])
            continue

        prod_entry.dateobs = metadata_basic['observation_date']
        prod_entry.uuid = metadata_basic['uuid']
        prod_entry.qc = metadata_basic['quality_control']
        session.add(prod_entry)

        print('compute tags')
        for k, v in metadata_basic['tags'].items():
            prod_entry[k] = v

    session.commit()

    print('processing observing blocks')
    for key, obs in obs_blocks.items():

        res = session.query(ObservingBlock).filter_by(id=obs.id).first()

        if res is not None:
            print('OB already inserted', obs.id)
            continue

        now = datetime.datetime.now()
        ob = ObservingBlock(instrument_id=obs.instrument, mode=obs.mode, start_time=now)
        ob.id = obs.id
        session.add(ob)

        for meta in obs.frames:
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
            ob.object = meta['object']

        query1 = session.query(Frame).join(ObservingBlock).filter(ObservingBlock.id == obs.id)
        res = query1.order_by(Frame.start_time).first()
        if res:
            ob.start_time = res.start_time

        res = query1.order_by(Frame.completion_time.desc()).first()
        if res:
            ob.completion_time = res.completion_time

        # Facts
        add_ob_facts(session, ob, ingestdir)

    # raw frames insertion
    for frame in raw_frames:
        call_event('on_ingest_raw_fits', session, frame, raw_frames[frame])

    session.commit()
