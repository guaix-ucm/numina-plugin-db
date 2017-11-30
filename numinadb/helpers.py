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

from __future__ import print_function

import os
import json

import numina.user.helpers
import numina.types.qc
from numina.types.product import DataProductTag
from numina.util.jsonencoder import ExtEncoder

from .model import DataProduct, ReductionResult, ReductionResultValue


def store_to(result, where):
    import numina.store
    print('calling my store_to')
    saveres = {}
    for key, prod in result.stored().items():
        val = getattr(result, key)

        where.destination = prod.dest
        saveres[key] = numina.store.dump(prod.type, val, where)

    return saveres


class ProcessingTask(numina.user.helpers.ProcessingTask):
    def __init__(self, session, obsres=None, runinfo=None):
        self.session = session
        super(ProcessingTask, self).__init__(obsres, runinfo)

    def store(self, where):
        print('calling store to ', where)
        # save to disk the RecipeResult part and return the file to save it
        # saveres = self.result.store_to(where)

        saveres = store_to(self.result, where)

        self.post_result_store(self.result, saveres)

        with open(where.result, 'w+') as fd:
            json.dump(saveres, fd, indent=2, cls=ExtEncoder)

        out = {}
        out['observation'] = self.observation
        out['result'] = where.result
        out['runinfo'] = self.runinfo

        logfile = 'processing.log'
        relpathdir = os.path.relpath(self.runinfo['results_dir'], self.runinfo['base_dir'])

        full_logfile = os.path.join(relpathdir, logfile)
        full_task = os.path.join(relpathdir, where.task)
        full_result = os.path.join(relpathdir, where.result)

        with open(where.task, 'w+') as fd:
            json.dump(out, fd, indent=2, cls=ExtEncoder)

        result = {'logs': full_logfile, 'task': full_task, 'result': full_result}

        return result

    def post_result_store(self, result, saveres):
        session = self.session

        result_db = ReductionResult()

        # print(self.runinfo)
        # print(self.observation)
        result_db.instrument_id = self.observation['instrument']

        result_db.pipeline = self.runinfo['pipeline']
        result_db.obsmode = self.observation['mode']
        result_db.recipe = self.runinfo['recipe_full_name']

        # datatype = Column(String(45))
        result_db.task_id = self.runinfo['taskid']
        # dateobs = Column(DateTime)
        if hasattr(result, 'qc'):
            result_db.qc = result.qc

        session.add(result_db)
        for key, prod in result.stored().items():
            if prod.dest != 'qc':

                val = ReductionResultValue()
                fullpath = os.path.join(self.runinfo['results_dir'], saveres[prod.dest])
                relpath = os.path.relpath(fullpath, self.runinfo['base_dir'])
                val.name = prod.dest
                val.datatype = prod.type.name()
                val.contents = relpath
                result_db.values.append(val)

                if isinstance(prod.type, DataProductTag):
                    product = DataProduct(datatype=prod.type.name(),
                                          task_id=self.runinfo['taskid'],
                                          instrument_id=self.observation['instrument'],
                                          contents=relpath
                                          )
                    product.result_value = val
                    internal_value = getattr(result, key)
                    meta_info = internal_value.meta
                    product.dateobs = meta_info['observation_date']
                    product.uuid = meta_info['uuid']
                    product.qc = meta_info['quality_control']
                    master_tags = meta_info['tags']
                    for k, v in master_tags.items():
                        product[k] = v

                    session.add(product)

        session.commit()

    def pre_result_store(self, result, saveres):
        session = self.session

        result_db = ReductionResult()
        result_db.instrument_id = self.observation['instrument'].name
        result_db.pipeline = self.runinfo['pipeline']
        result_db.obsmode = self.observation['mode']
        result_db.recipe = self.runinfo['recipe_full_name']
        result_db.task_id = self.runinfo['taskid']
        # dateobs = Column(DateTime)
        if hasattr(result, 'qc'):
            result_db.qc = result.qc

        session.add(result_db)

        for key, prod in result.stored().items():
            if prod.dest != 'qc':

                val = ReductionResultValue()
                fullpath = os.path.join(self.runinfo['results_dir'], saveres[prod.dest])
                relpath = os.path.relpath(fullpath, self.runinfo['base_dir'])
                val.name = prod.dest
                val.datatype = prod.type.name()
                val.contents = relpath
                result_db.values.append(val)

                if isinstance(prod.type, DataProductTag):
                    product = DataProduct(datatype=prod.type.name(),
                                          task_id=self.runinfo['taskid'],
                                          instrument_id=self.observation['instrument'],
                                          contents=relpath
                                          )
                    product.result_value = val
                    internal_value = getattr(result, key)
                    print('extract meta info', prod.type, fullpath, internal_value)
                    meta_info = prod.type.extract_db_info(fullpath)
                    product.dateobs = meta_info['observation_date']
                    product.uuid = meta_info['uuid']
                    product.qc = meta_info['quality_control']
                    master_tags = meta_info['tags']
                    for k, v in master_tags.items():
                        product[k] = v

                    session.add(product)

        session.commit()


def build_mdir(taskid, obsid):
    mdir = "task_{0:03d}_{1}".format(taskid, obsid)
    return mdir


class WorkEnvironment(numina.user.helpers.WorkEnvironment):
    def __init__(self, basedir, datadir, taskid, task_obid):
        mdir = build_mdir(taskid, task_obid)
        workdir = os.path.join(basedir, mdir, 'work')
        resultsdir = os.path.join(basedir, mdir, 'results')

        if datadir is None:
            datadir = os.path.join(basedir, 'data')

        super(WorkEnvironment, self).__init__("1", basedir, workdir, resultsdir, datadir)
