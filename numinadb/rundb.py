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

from sqlalchemy import create_engine
from numina.user.helpers import ProcessingTask, WorkEnvironment, DiskStorageDefault

from .model import Base
from .model import Task
from .dal import SqliteDAL, Session

import ConfigParser

_logger = logging.getLogger("numina")

class MyW(WorkEnvironment):
    def __init__(self, basedir, datadir, task):
        mdir = "task_{0.id}_{0.ob_id}".format(task)
        workdir = os.path.join(basedir, mdir, 'work')
        resultsdir = os.path.join(basedir, mdir, 'results')
        super(MyW, self).__init__(basedir, workdir, resultsdir, datadir)

def register(subparsers, config):

    try:
        db_default = config.get('rundb', 'database')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        db_default = 'processing.db'

    try:
        ddir_default = config.get('rundb', 'datadir')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        ddir_default = None

    try:
        bdir_default = config.get('rundb', 'basedir')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
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

    #cwd = os.getcwd()
    #os.chdir(args.datadir)

    _logger.debug("pipeline from CLI is %r", args.pipe_name)
    pipe_name = args.pipe_name

    #obsres = dal.obsres_from_oblock_id(args.obid)


    # Direct query to insert a new task
    session = Session()
    #newtask = Task(ob_id=obsres.id)
    #session.add(newtask)
    #session.commit()

    class A(object):
        pass

    newtask = A()
    newtask.id = 1
    newtask.ob_id = 2

    workenv = MyW(args.basedir, args.datadir, newtask)
    print(workenv)

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

    runinfo = {'pipeline': vpipe_name,
               'recipeclass': vrecipeclass,
               'workenv': workenv,
               'recipe_version': vrecipe.__version__,
               'instrument_configuration': None
               }

    task = ProcessingTask(obsres, runinfo)

    # Copy files
    _logger.debug('copy files to work directory')
    workenv.sane_work()
    workenv.copyfiles_stage1(obsres)
    workenv.copyfiles_stage2(rinput)

    completed_task = run_recipe(recipe=recipe,task=task, rinput=rinput,
                                workenv=workenv, task_control=task_control)

    where = DiskStorageDefault(resultsdir=workenv.resultsdir)

    where.store(completed_task)

def create_recipe_file_logger(logger, logfile, logformat):
    '''Redirect Recipe log messages to a file.'''
    recipe_formatter = logging.Formatter(logformat)
    fh = logging.FileHandler(logfile, mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(recipe_formatter)
    return fh


def run_recipe(recipe, task, rinput, workenv, task_control):
    """Recipe execution mode of numina."""

    # Creating custom logger file
    DEFAULT_RECIPE_LOGGER = 'numina.recipes'
    recipe_logger = logging.getLogger(DEFAULT_RECIPE_LOGGER)

    logger_control = task_control['logger']
    if logger_control['enabled']:
        logfile = os.path.join(workenv.resultsdir, logger_control['logfile'])
        logformat = logger_control['format']
        _logger.debug('creating file logger %r from Recipe logger', logfile)
        fh = create_recipe_file_logger(recipe_logger, logfile, logformat)
    else:
        fh = logging.NullHandler()

    recipe_logger.addHandler(fh)


    csd = os.getcwd()
    _logger.debug('cwd to workdir')
    os.chdir(workenv.workdir)
    try:
        completed_task = run_recipe_timed(recipe, rinput, task)

        return completed_task

    finally:
        _logger.debug('cwd to original path: %r', csd)
        os.chdir(csd)
        recipe_logger.removeHandler(fh)


def run_recipe_timed(recipe, rinput, task):
    """Run the recipe and count the time it takes."""
    TIMEFMT = '%FT%T'
    _logger.info('running recipe')
    now1 = datetime.datetime.now()
    task.runinfo['time_start'] = now1.strftime(TIMEFMT)
    #

    result = recipe.run(rinput)
    _logger.info('result: %r', result)
    task.result = result
    #
    now2 = datetime.datetime.now()
    task.runinfo['time_end'] = now2.strftime(TIMEFMT)
    task.runinfo['time_running'] = now2 - now1
    return task

