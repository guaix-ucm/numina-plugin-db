#
# Copyright 2016-2025 Universidad Complutense de Madrid
#
# This file is part of Numina DB
#
# SPDX-License-Identifier: GPL-3.0-or-later
# License-Filename: LICENSE.txt
#

"""User command line interface of Numina."""


import logging
import os

import numina.drps
from numina.store import load
from numina.dal.absdal import AbsDrpDAL
from numina.exceptions import NoResultFound
from numina.dal.stored import StoredProduct, StoredParameter
from numina.dal.utils import tags_are_valid
from numina.core import DataFrameType

from .model import ObservingBlock, DataProduct, RecipeParameters, ObservingBlockAlias
from .model import DataProcessingTask, ReductionResult

_logger = logging.getLogger("numina.db.dal")


def search_oblock_from_id(session, obsref):

    # Search possible alias
    alias_res = session.query(ObservingBlockAlias).filter_by(alias=obsref).first()
    if alias_res:
        obsid = alias_res.uuid
    else:
        obsid = obsref

    res = session.query(ObservingBlock).filter(ObservingBlock.id == obsid).one()
    if res:
        return res
    else:
        raise NoResultFound("oblock with id %d not found" % obsid)


class SqliteDAL(AbsDrpDAL):
    def __init__(self, dialect, session, basedir, datadir):
        drps = numina.drps.get_system_drps()
        super(SqliteDAL, self).__init__(drps)

        self.dialect = dialect
        self.session = session
        self.basedir = basedir
        self.datadir = datadir
        self.extra_data = {}

    def search_oblock_from_id(self, obsref):

        return search_oblock_from_id(self.session, obsref)

    def search_prod_obsid(self, ins, obsid, pipeline):
        """Returns the first coincidence..."""
        ins_prod = None  # self.prod_table[ins]

        # search results of these OBs
        for prod in ins_prod.values():
            if prod['ob'] == obsid:
                # We have found the result, no more checks
                return StoredProduct(**prod)
        else:
            raise NoResultFound('result for ob %i not found' % obsid)

    def search_prod_req_tags(self, req, ins, tags, pipeline):
        return self.search_prod_type_tags(req.type, ins, tags, pipeline)

    def search_prod_type_tags(self, tipo, ins, tags, pipeline):
        """Returns the first coincidence..."""

        _logger.debug('query search_prod_type_tags type=%s instrument=%s tags=%s pipeline=%s',
                      tipo, ins, tags, pipeline)
        # drp = self.drps.query_by_name(ins)
        label = tipo.name()
        # print('search prod', tipo, ins, tags, pipeline)
        session = self.session
        # FIXME: and instrument == ins
        res = session.query(DataProduct).filter(DataProduct.datatype == label).order_by(DataProduct.priority.desc())
        _logger.debug('requested tags are %s', tags)
        for prod in res:
            pt = {}
            # TODO: facts should be a dictionary
            for key, val in prod.facts.items():
                pt[val.key] = val.value
            # print('product ', prod.id, 'tags', pt)
            _logger.debug('found value with id %d', prod.id)
            _logger.debug('product tags are %s', pt)

            if tags_are_valid(pt, tags):
                _logger.debug('tags are valid, return product, id=%s', prod.id)
                _logger.debug('content is %s', prod.contents)
                # this is a valid product
                return StoredProduct(id=prod.id,
                                     content=load(tipo, os.path.join(self.basedir, prod.contents)),
                                     tags=pt
                                     )
            _logger.debug('tags are in valid')
        else:
            _logger.debug('query search_prod_type_tags, no result found')
            msg = 'type %s compatible with tags %r not found' % (label, tags)
            raise NoResultFound(msg)

    def search_param_type_tags(self, name, tipo, instrument, mode, pipeline, tags):
        _logger.debug('query search_param_type_tags name=%s instrument=%s tags=%s '
                      'pipeline=%s mode=%s', name, instrument, tags, pipeline, mode)
        session = self.session

        if isinstance(instrument, str):
            instrument_id = instrument
        else:
            instrument_id = instrument.name

        res = session.query(RecipeParameters).filter(
            RecipeParameters.instrument_id == instrument_id,
            RecipeParameters.pipeline == pipeline,
            RecipeParameters.name == name,
            RecipeParameters.mode == mode).one_or_none()
        _logger.debug('requested tags are %s', tags)
        if res is None:
            raise NoResultFound("No parameters for %s mode, pipeline %s", mode, pipeline)
        for value in res.values:
            pt = {}
            for f in value.facts.values():
                pt[f.key] = f.value
            _logger.debug('found value with id %d', value.id)
            _logger.debug('param tags are %s', pt)

            if tags_are_valid(pt, tags):
                _logger.debug('tags are valid, param, id=%s, end', value.id)
                _logger.debug('content is %s', value.content)
                # this is a valid product
                return StoredParameter(value.content)
        else:
            raise NoResultFound("No parameters for %s mode, pipeline %s", mode, pipeline)

    def obsres_from_oblock_id(self, obsid, override_mode=None):
        # Search
        _logger.debug('query obsres_from_oblock_id with obsid=%s', obsid)
        obsres = self.search_oblock_from_id(obsid)

        if override_mode:
            obsres.mode = override_mode
        if obsres.instrument is None:
            raise ValueError('Undefined Instrument')

        this_drp = self.drps.query_by_name(obsres.instrument)

        mode = this_drp.modes[obsres.mode]
        tagger = mode.tagger

        if tagger is None:
            master_tags = {}
        else:
            master_tags = tagger(obsres)

        obsres.tags = master_tags
        obsres.configuration = this_drp.configuration_selector(obsres)
        obsres.pipeline = 'default'

        return obsres

    def search_parameter(self, name, tipo, obsres, options=None):
        # returns StoredProduct
        instrument = obsres.instrument
        mode = obsres.mode
        tags = obsres.tags
        pipeline = obsres.pipeline

        if name in self.extra_data:
            value = self.extra_data[name]
            content = StoredParameter(value)
            return content
        else:
            return self.search_param_type_tags(name, tipo, instrument, mode, pipeline, tags)

    def search_product(self, name, tipo, obsres, options=None):
        # returns StoredProduct
        ins = obsres.instrument
        tags = obsres.tags
        pipeline = obsres.pipeline

        if name in self.extra_data:
            val = self.extra_data[name]
            content = load(tipo, val)
            return StoredProduct(id=0, tags={}, content=content)
        else:
            return self.search_prod_type_tags(tipo, ins, tags, pipeline)

    def search_result_relative(self, name, tipo, obsres, mode, field, node, options=None):
        # So, if node is children, I have to obtain
        session = self.session
        if node == 'children':
            print('obtain', field, 'from all the children of', obsres.taskid)
            res = session.query(DataProcessingTask).filter_by(id=obsres.taskid).one()
            result = []
            for child in res.children:
                # this can be done better...
                nodes = session.query(ReductionResult).filter_by(task_id=child.id).first()
                # this surely can be a mapping instead of a list

                for prod in nodes.values:
                    if prod.name == field:
                        st = StoredProduct(
                            id=prod.id,
                            content=load(DataFrameType(), os.path.join(self.basedir, prod.contents)),
                            tags={}
                        )
                        result.append(st)
                        break
            return result

        elif node == 'prev':
            print('obtain', field, 'from the previous node to', obsres.taskid)
            res = session.query(DataProcessingTask).filter_by(id=obsres.taskid).one()
            # inspect children of my parent
            parent = res.parent
            if parent:
                print([child for child in parent.children])
                raise NoResultFound
            else:
                # Im top level, no previous
                raise NoResultFound
        else:
            pass  # print(dest, type, obsres, mode, field, node)
