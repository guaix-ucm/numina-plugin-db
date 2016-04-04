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
import datetime

import six.moves.configparser as configparser
from sqlalchemy import create_engine
from numina.user.helpers import ProcessingTask, WorkEnvironment, DiskStorageDefault
from numina.user.clirundal import run_recipe
from numina.core.products import DataProductTag

from .model import Base
from .model import Task
from .model import DataProduct
from .dal import SqliteDAL, Session


_logger = logging.getLogger("numina")

import yaml

class MyT(ProcessingTask):

    def store(self, where):
        # save to disk the RecipeResult part and return the file to save it

        result = self.result

        saveres = self.result.store_to(where)

        self.post_result_store(result, saveres)

        with open(where.result, 'w+') as fd:
            yaml.dump(saveres, fd)

        self.result = where.result

        with open(where.task, 'w+') as fd:
            yaml.dump(self.__dict__, fd)
        return where.task

    def post_result_store(self, result, saveres):

        session = Session()

        for key, prod in result.stored().items():
            if prod.dest != 'qc' and isinstance(prod.type, DataProductTag):
                #
                product = DataProduct(datatype=prod.type.__class__.__name__,
                                      task_id=self.runinfo['taskid'],
                                      instrument_id='MEGARA',
                                      contents=saveres[prod.dest]
                                      )

                session.add(product)
        session.commit()


class MyW(WorkEnvironment):
    def __init__(self, basedir, datadir, task):
        mdir = "task_{0.id}_{0.ob_id}".format(task)
        workdir = os.path.join(basedir, mdir, 'work')
        resultsdir = os.path.join(basedir, mdir, 'results')
        super(MyW, self).__init__(basedir, workdir, resultsdir, datadir)


def register(subparsers, config):

    try:
        db_default = config.get('rundb', 'database')
    except (configparser.NoSectionError, configparser.NoOptionError):
        db_default = 'processing.db'

    try:
        ddir_default = config.get('rundb', 'datadir')
    except (configparser.NoSectionError, configparser.NoOptionError):
        ddir_default = None

    try:
        bdir_default = config.get('rundb', 'basedir')
    except (configparser.NoSectionError, configparser.NoOptionError):
        bdir_default = os.getcwd()

    parser_run = subparsers.add_parser(
        'rundb',
        help='process a observation result from a database'
        )

    sub = parser_run.add_subparsers(description='rundb-sub', dest='sub')

    parser_db = sub.add_parser('db')
    # parser_run.set_defaults(command=mode_db)
    parser_db.add_argument('--initdb', nargs='?',
                           default=db_default,
                           const=db_default,
                           metavar='URI', 
                           help='Create a database')
    parser_db.set_defaults(command=mode_db)

    parser_id = sub.add_parser('id')
    parser_id.add_argument('obid')
    parser_id.add_argument('--db',
                           default=db_default,
                           dest='db_uri',
                           metavar='URI', 
                           help='Path to the database')
    parser_id.add_argument(
        '-p', '--pipeline', dest='pipe_name',
        default='default', help='name of a pipeline'
        )
    parser_id.add_argument(
        '--basedir', action="store", dest="basedir",
        default=bdir_default,
        help='path to create the following directories'
        )
    parser_id.add_argument(
        '--datadir', action="store", dest="datadir", default=ddir_default,
        help='path to directory containing pristine data'
        )
    parser_id.set_defaults(command=mode_run_db)
    return parser_run


def mode_db(args):
    if args.initdb is not None:
        print('Create database in', args.initdb)
        create_sqlite_db(filename=args.initdb)


def create_sqlite_db(filename):

    uri = 'sqlite:///%s' % filename
    engine = create_engine(uri, echo=False)

    Base.metadata.create_all(engine)


def mode_run_db(args):
    mode_run_common_obs(args)
    return 0


def mode_run_common_obs(args):
    """Observing mode processing mode of numina."""

    uri = 'sqlite:///%s' % args.db_uri
    engine = create_engine(uri, echo=False)
    # DAL must use the database
    dal = SqliteDAL(engine)

    # Directories with relevant data


    _logger.debug("pipeline from CLI is %r", args.pipe_name)
    pipe_name = args.pipe_name

    obsres = dal.obsres_from_oblock_id(args.obid)

    # Direct query to insert a new task
    session = Session()
    dbtask = Task(ob_id=obsres.id)
    session.add(dbtask)
    session.commit()

    workenv = MyW(args.basedir, args.datadir, dbtask)

    cwd = os.getcwd()
    os.chdir(workenv.datadir)

    recipeclass = dal.search_recipe_from_ob(obsres, pipe_name)
    _logger.debug('recipe class is %s', recipeclass)

    rinput = recipeclass.build_recipe_input(obsres, dal, pipeline=pipe_name)

    os.chdir(cwd)

    recipe = recipeclass()
    _logger.debug('recipe created')

    # Logging and task control
    logger_control = dict(
        logfile='processing.log',
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        enabled=True
        )

    # Load recipe control and recipe parameters from file
    task_control = dict(requirements={}, products={}, logger=logger_control)

    # Build the recipe input data structure
    # and copy needed files to workdir
    _logger.debug('parsing requirements')
    for key in recipeclass.requirements().values():
        _logger.info("recipe requires %r", key)

    _logger.debug('parsing products')
    for req in recipeclass.products().values():
        _logger.info('recipe provides %r', req)

    runinfo = {
        'taskid': dbtask.id,
        'pipeline': pipe_name,
               'recipeclass': recipeclass,
               'workenv': workenv,
               'recipe_version': recipe.__version__,
               'instrument_configuration': None
               }

    task = MyT(obsres, runinfo)

    # Copy files
    _logger.debug('copy files to work directory')
    workenv.sane_work()
    workenv.copyfiles_stage1(obsres)
    workenv.copyfiles_stage2(rinput)

    completed_task = run_recipe(recipe=recipe,task=task, rinput=rinput,
                                workenv=workenv, task_control=task_control)

    where = DiskStorageDefault(resultsdir=workenv.resultsdir)

    where.store(completed_task)

    dbtask.completion_time = datetime.datetime.now()
    session.commit()
