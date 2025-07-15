#
# Copyright 2016-2017 Universidad Complutense de Madrid
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

import sys
import os
import logging
import datetime

import pkg_resources
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from numina.user.helpers import DiskStorageDefault
from numina.util.context import working_directory
from numina.user.clirundal import run_recipe

from .model import Base
from .model import DataProcessingTask

from .dal import SqliteDAL, search_oblock_from_id
from .helpers import ProcessingTask, WorkEnvironment
from .control import mode_alias_add, mode_alias_del, mode_alias_list
from .ingest import ingest_ob_file, ingest_dir, ingest_control_file


Session = sessionmaker()

runner = 'numina-plugin-db'
runner_version = '1'


_logger = logging.getLogger("numina.db")


def complete_config(config):
    """Complete config with default values"""

    if not config.has_section('rundb'):
        config.add_section('rundb')

    values = {
        'database': 'sqlite:///processing.db',
        'datadir': "",
        'basedir': os.getcwd(),
    }

    for k, v in values.items():
        if not config.has_option('rundb', k):
            config.set('rundb', k, v)

    return config


def register(subparsers, config):

    complete_config(config)

    db_default = config.get('rundb', 'database')
    ddir_default = config.get('rundb', 'datadir')
    bdir_default = config.get('rundb', 'basedir')

    if ddir_default == "":
        ddir_default = None

    parser_rundb = subparsers.add_parser(
        'rundb',
        help='process a observation result from a database'
        )

    parser_rundb.add_argument('--db',
                              default=db_default,
                              dest='db_uri',
                              metavar='URI',
                              help='Path to the database'
                              )

    subdb = parser_rundb.add_subparsers(
        title='DB Targets',
        description='These are valid commands you can ask numina rundb to do.'
    )

    parser_alias = subdb.add_parser('alias', help='manage alias to OB names')
    # Adding alias

    subalias = parser_alias.add_subparsers(
        title='Alias Targets',
        help='alias commands'
    )

    parser_alias_add = subalias.add_parser('add', help='add alias')
    parser_alias_add.add_argument('--force', action='store_true', help='force adding the alias')
    parser_alias_add.add_argument('aliasname')
    parser_alias_add.add_argument('uuid', nargs='?')
    parser_alias_add.set_defaults(command=mode_alias, action='add')

    parser_alias_del = subalias.add_parser('delete', help='delete alias')
    parser_alias_del.add_argument('aliasname')
    parser_alias_del.set_defaults(command=mode_alias, action='del')

    parser_alias_list = subalias.add_parser('list', help='list alias')
    parser_alias_list.set_defaults(command=mode_alias, action='list')

    parser_db = subdb.add_parser('db', help='manage database')
    # parser_run.set_defaults(command=mode_db)
    parser_db.add_argument('--initdb', nargs='?',
                           default=None,
                           const=db_default,
                           metavar='URI',
                           help='Create a database')

    parser_db.set_defaults(command=mode_db)

    parser_id = subdb.add_parser('id', help='run reductions based on OB id')
    parser_id.add_argument('obid')
    parser_id.add_argument('--query',
                           help='Query')
    parser_id.add_argument(
        '-p', '--pipeline', dest='pipe_name',
        default='default', help='name of a pipeline'
        )
    parser_id.add_argument(
        '--mode', dest='mode_name',
        help='override observing mode'
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

    parser_ingest = subdb.add_parser('ingest', help='ingest data in the database')
    parser_ingest.add_argument('--ob-file', action='store_true')
    parser_ingest.add_argument('--control-file', action='store_true')
    parser_ingest.add_argument('path')

    parser_ingest.set_defaults(command=mode_ingest)

    load_entry_points()

    return parser_rundb


def load_entry_points():
    entry = 'numinadb.extra.1'
    for entry in pkg_resources.iter_entry_points(group=entry):
        try:
            entry.load()
        except Exception as error:
            print('Problem loading', entry, file=sys.stderr)
            print("Error is: ", error, file=sys.stderr)


def mode_db(args, extra_args):

    if args.initdb is not None:
        print('Create database in', args.initdb)
        create_db(uri=args.initdb)


def create_db(uri):
    engine = create_engine(uri, echo=False)
    Base.metadata.create_all(bind=engine)


def mode_run_db(args, extra_args):
    mode_run_common_obs(args, extra_args)
    return 0


def mode_alias(args, extra_args):

    engine = create_engine(args.db_uri, echo=False)
    Session.configure(bind=engine)
    session = Session()

    if args.action == 'add':
        mode_alias_add(session, args.aliasname, args.uuid, force=args.force)

    elif args.action == 'del':
        mode_alias_del(session, args.aliasname)

    elif args.action == 'list':
        mode_alias_list(session)
    else:
        pass


def run_task(session, task, dal):

    if task.state == 2:
        print('already done')
        return

    # Run on children first
    for child in task.children:
        run_task(session, child, dal)

    # Check all my children are: awaited: False
    # Check all my children are: state: 2 # FINISHED
    for child in task.children:
        if child.awaited:
            print('im running and', child.id, 'is awaited')
            raise ValueError('nor awaited')
    else:
        print('im running and nothing is awaited')
        task.waiting = False

    # setup things
    task.start_time = datetime.datetime.utcnow()
    task.state = 1
    task_method = methods[task.method]

    try:
        result = task_method(request=task.request, dal=dal, taskid=task.id)
        task.result = result
        # On completion
        task.state = 2
        task.awaited = False
    except Exception:
        task.state = 3
        raise
    finally:
        task.completion_time = datetime.datetime.utcnow()
        session.commit()


def mode_run_common_obs(args, extra_args):
    """Observing mode processing mode of numina."""

    engine = create_engine(args.db_uri, echo=False)
    Session.configure(bind=engine)
    session = Session()

    print('generate reduction tasks')
    request_params = {}
    if args.mode_name:
        request_params['mode_override'] = args.mode_name
    request_params['pipeline'] = args.pipe_name
    task = generate_reduction_tasks(session, args.obid, request_params)

    # query
    # tasks = session.query(DataProcessingTask).filter_by(label='root', state=0)

    # DAL must use the database
    if args.datadir is None:
        datadir = os.path.join(args.basedir, 'data')
    else:
        datadir = args.datadir

    dal = SqliteDAL(runner, session, basedir=args.basedir, datadir=datadir)
    _logger.debug("DAL is %s with datadir=%s", type(dal), datadir)

    # Directories with relevant data
    # pipe_name = 'default'

    print('start')
    run_task(session, task, dal)
    print('end', task.completion_time)
    session.commit()


def mode_ingest(args, extra_args):

    engine = create_engine(args.db_uri, echo=False)
    Session.configure(bind=engine)
    session = Session()

    if args.control_file:
        ingest_control_file(session, args.path)
        return

    if args.ob_file:
        ingest_ob_file(session, args.path)
        return
    else:
        ingest_dir(session, args.path)
        return


def reductionOB(**kwargs):
    print('reductionOB')
    dal = kwargs['dal']
    taskid = kwargs['taskid']
    request = kwargs['request']
    #
    print('request is:', request)
    obid = request['id']
    pipe_name = request.get('pipe_name', 'default')
    mode_name = request.get('mode_override')

    return reductionOB_request(dal, taskid, obid,
                               mode_name=mode_name,
                               pipe_name=pipe_name
                               )


def reductionOB_request(dal, taskid, obid, mode_name=None, pipe_name='default'):

    session = dal.session
    datadir = dal.datadir
    basedir = dal.basedir

    with working_directory(datadir):
        obsres = dal.obsres_from_oblock_id(obid,
                                           override_mode=mode_name
                                           )

    workenv = WorkEnvironment(basedir, datadir, taskid, obid)

    with working_directory(workenv.datadir):
        recipe = dal.search_recipe_from_ob(obsres)

        # Enable intermediate results by default
        _logger.debug('enable intermediate results')
        recipe.intermediate_results = True

        # Update runinfo
        _logger.debug('update recipe runinfo')
        recipe.runinfo['runner'] = runner
        recipe.runinfo['runner_version'] = runner_version
        recipe.runinfo['taskid'] = taskid
        recipe.runinfo['data_dir'] = workenv.datadir
        recipe.runinfo['work_dir'] = workenv.workdir
        recipe.runinfo['results_dir'] = workenv.resultsdir
        recipe.runinfo['base_dir'] = workenv.basedir

        try:
            # uhmmm
            obsres.taskid = taskid
            rinput = recipe.build_recipe_input(obsres, dal)
        except ValueError as err:
            _logger.error("during recipe input construction")
            for msg in err.args[0]:
                _logger.error(msg)
            raise

        _logger.debug('recipe input created')
        # Build the recipe input data structure
        # and copy needed files to workdir
        _logger.debug('parsing requirements')
        for key in recipe.requirements():
            v = getattr(rinput, key)
            _logger.info("recipe requires %r value is %r", key, v)

        _logger.debug('parsing products')
        for req in recipe.products().values():
            _logger.info('recipe provides %s, %s', req.type, req.description)

    # Logging and task control
    logger_control = dict(
        logfile='processing.log',
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        enabled=True
        )

    # Load recipe control and recipe parameters from file
    task_control = dict(requirements={}, products={}, logger=logger_control)

    runinfo = {
        'taskid': taskid,
        'pipeline': pipe_name,
        'recipeclass': recipe.__class__,
        'workenv': workenv,
        'recipe_version': recipe.__version__,
        'runner': runner,
        'runner_version': runner_version,
        'instrument_configuration': None
    }

    task = ProcessingTask(session, obsres, runinfo)

    # Copy files
    if True:
        _logger.debug('copy files to work directory')
        workenv.sane_work()
        workenv.copyfiles_stage1(obsres)
        workenv.copyfiles_stage2(rinput)
        workenv.adapt_obsres(obsres)
    # link files
    else:
        _logger.debug('link files to work directory')
        workenv.sane_work()
        workenv.copyfiles_stage1(obsres)
        workenv.copyfiles_stage2(rinput)
        workenv.adapt_obsres(obsres)

    completed_task = run_recipe(recipe=recipe, task=task, rinput=rinput,
                                workenv=workenv, task_control=task_control)

    where = DiskStorageDefault(resultsdir=workenv.resultsdir)
    where.task = 'task.json'
    where.result = 'result.json'

    result = where.store(completed_task)
    return result


def reduction(**kwargs):
    print('reduction', kwargs)
    return 'something'


methods = {}
methods['reductionOB'] = reductionOB
methods['reduction'] = reduction


def generate_reduction_tasks(session, obid, request_params):
    """Generate reduction tasks."""

    obsres = search_oblock_from_id(session, obid)

    request = {"id": obid}
    request.update(request_params)

    # Generate Main Reduction Task
    print('generate main task')
    dbtask = DataProcessingTask()
    dbtask.host = 'localhost'
    dbtask.label = 'root'
    dbtask.awaited = False
    dbtask.waiting = True
    dbtask.method = 'reduction'
    dbtask.request = request
    dbtask.ob = obsres
    print('generate done')
    session.add(dbtask)
    # Generate reductionOB
    #
    print('generate recursive')
    recursive_tasks(dbtask, obsres, request_params)

    session.commit()
    return dbtask


def recursive_tasks(parent_task, obsres, request_params):

    request = {"id": obsres.id}
    request.update(request_params)

    dbtask = DataProcessingTask()
    dbtask.host = 'localhost'
    dbtask.label = 'node'
    dbtask.awaited = False
    dbtask.waiting = False
    dbtask.method = 'reductionOB'
    dbtask.request = request
    dbtask.ob = obsres
    if parent_task:
        dbtask.awaited = True
        parent_task.children.append(dbtask)

    for ob in obsres.children:
        recursive_tasks(dbtask, ob, request_params)

    if obsres.children:
        dbtask.waiting = True
