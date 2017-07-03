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

import yaml
import numina.user.helpers
from numina.core.products import DataProductTag

from .model import DataProduct
from .model import Fact
from .dal import Session


class ProcessingTask(numina.user.helpers.ProcessingTask):
    def __init__(self, obsres=None, insconf=None):
        super(ProcessingTask, self).__init__(obsres, insconf)
        # Additionally
        self.obsres = obsres

    def store(self, where):
        # save to disk the RecipeResult part and return the file to save it

        saveres = self.result.store_to(where)

        self.post_result_store(self.result, saveres)

        with open(where.result, 'w+') as fd:
            yaml.dump(saveres, fd)

        out = {}
        out['observation'] = self.observation
        out['result'] = where.result
        out['runinfo'] = self.runinfo

        with open(where.task, 'w+') as fd:
            yaml.dump(out, fd)
        return where.task

    def post_result_store(self, result, saveres):

        mdir = build_mdir(self.runinfo['taskid'], self.observation['observing_result'])

        session = Session()

        for key, prod in result.stored().items():
            if prod.dest != 'qc' and isinstance(prod.type, DataProductTag):
                #
                product = DataProduct(datatype=prod.type.__class__.__name__,
                                      task_id=self.runinfo['taskid'],
                                      instrument_id='MEGARA',
                                      contents=os.path.join(mdir, 'results', saveres[prod.dest])
                                      )

                for k, v in self.obsres.tags.items():
                    fact = session.query(Fact).filter_by(key=k, value=v).first()
                    if fact is None:
                        fact = Fact(key=k, value=v)

                    product.facts.append(fact)

                session.add(product)

        session.commit()


def build_mdir(taskid, obsid):
    mdir = "task_{0}_{1}".format(taskid, obsid)
    return mdir


class WorkEnvironment(numina.user.helpers.WorkEnvironment):
    def __init__(self, basedir, datadir, task):
        mdir = build_mdir(task.id, task.ob_id)
        workdir = os.path.join(basedir, mdir, 'work')
        resultsdir = os.path.join(basedir, mdir, 'results')

        if datadir is None:
            datadir = os.path.join(basedir, 'data')

        super(WorkEnvironment, self).__init__("1", basedir, workdir, resultsdir, datadir)

