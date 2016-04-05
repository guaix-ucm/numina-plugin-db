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


import os
import logging

from sqlalchemy.orm import sessionmaker

from numina.core import import_object

from numina.core.pipeline import DrpSystem
from numina.store import load
from numina.dal import AbsDAL
from numina.exceptions import NoResultFound
from numina.dal.stored import ObservingBlock
from numina.dal.stored import StoredProduct, StoredParameter
from numina.core import fully_qualified_name

from .model import MyOb, DataProduct

Session = sessionmaker()

_logger = logging.getLogger("numina")


def product_label(drp, klass):
    fqn = fully_qualified_name(klass)
    for p in drp.products:
        if p['name'] == fqn:
            return p['alias']
    else:
        return klass.__name__


def tags_are_valid(subset, superset):
    for key, val in subset.items():
        if key in superset and superset[key] != val:
            return False
    return True


class SqliteDAL(AbsDAL):
    def __init__(self, engine, basedir, datadir):
        super(SqliteDAL, self).__init__()
        self.drps = DrpSystem()
        Session.configure(bind=engine)
        self.basedir = basedir
        self.datadir = datadir

    def search_oblock_from_id(self, obsid):
        session = Session()
        res = session.query(MyOb).filter(MyOb.id == obsid).one()
        if res:
            thisframes = [frame.to_numina_frame() for frame in res.frames]
            return ObservingBlock(res.id,
                                  res.instrument,
                                  res.mode,
                                  thisframes,
                                  children=[],
                                  parent=None,
                                  facts=res.facts)
        else:
            raise NoResultFound("oblock with id %d not found" % obsid)

    def search_recipe(self, ins, mode, pipeline):
        recipe_fqn = self.search_recipe_fqn(ins, mode, pipeline)
        klass = import_object(recipe_fqn)
        return klass

    def search_recipe_fqn(self, ins, mode, pipename):

        drp = self.drps.query_by_name(ins)

        this_pipeline = drp.pipelines[pipename]
        recipes = this_pipeline.recipes
        recipe_fqn = recipes[mode]
        return recipe_fqn

    def search_recipe_from_ob(self, ob, pipeline):
        ins = ob.instrument
        mode = ob.mode
        return self.search_recipe(ins, mode, pipeline)

    def search_prod_obsid(self, ins, obsid, pipeline):
        '''Returns the first coincidence...'''
        ins_prod = None # self.prod_table[ins]

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

        klass = tipo.__class__
        drp = self.drps.query_by_name(ins)
        label = product_label(drp, klass)
        # print('search prod', tipo, ins, tags, pipeline)
        session = Session()
        # FIXME: and instrument == ins
        res = session.query(DataProduct).filter(DataProduct.datatype == label)

        for prod in res:
            pt = {}
            # FIXME: facts should be a dictionary
            for f in prod.facts:
                pt[f.key] = f.value
            # print('product ', prod.id, 'tags', pt)
            #print prod.facts
            #pt = {}
            if tags_are_valid(pt, tags):
                # this is a valid product
                return StoredProduct(id=prod.id,
                                     content=load(tipo, os.path.join(self.basedir, prod.contents)),
                                     tags=pt
                                     )
        else:
            # print('not found, raise')
            msg = 'type %s compatible with tags %r not found' % (klass, tags)
            raise NoResultFound(msg)

    def search_param_req(self, req, instrument, mode, pipeline):
        # FIXME: a table with parameters...
        # self.req_table = None
        # req_table_ins = self.req_table.get(instrument, {})
        # req_table_insi_pipe = req_table_ins.get(pipeline, {})
        # mode_keys = req_table_insi_pipe.get(mode, {})
        # if req.dest in mode_keys:
        #     value = mode_keys[req.dest]
        #     content = StoredParameter(value)
        #     return content
        # else:
        if True:
            raise NoResultFound("No parameters for %s mode, pipeline %s", mode, pipeline)

    def obsres_from_oblock_id(self, obsid):
        # Search
        obsres = self.search_oblock_from_id(obsid)

        # Fill tags
        obsres.tags = {}

        for fact in obsres.facts:
            obsres.tags[fact.key] = fact.value

        return obsres


