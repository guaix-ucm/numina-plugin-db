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
import os
import logging
import datetime

from sqlalchemy import create_engine
from numina.user.helpers import DiskStorageDefault
from numina.user.clirundal import run_recipe
from numina.core.oresult import ObservationResult

from .model import Base
from .model import Task
from .dal import SqliteDAL, Session
from .helpers import ProcessingTask, WorkEnvironment

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

    parser_ingest = sub.add_parser('ingest')
    parser_ingest.add_argument('path')

    parser_ingest.set_defaults(command=mode_ingest)

    return parser_run


def mode_db(args, extra_args):
    if args.initdb is not None:
        print('Create database in', args.initdb)
        create_db(uri=args.initdb)


def create_db(uri):
    engine = create_engine(uri, echo=False)
    Base.metadata.create_all(engine)


def mode_run_db(args, extra_args):
    mode_run_common_obs(args)
    return 0


def mode_run_common_obs(args):
    """Observing mode processing mode of numina."""

    engine = create_engine(args.db_uri, echo=False)
    # DAL must use the database
    if args.datadir is None:
        datadir = os.path.join(args.basedir, 'data')
    else:
        datadir = args.datadir

    dal = SqliteDAL(engine, basedir=args.basedir, datadir=datadir)
    _logger.debug("DAL is %s with datadir=%s", type(dal), datadir)

    # Directories with relevant data
    _logger.debug("pipeline from CLI is %r", args.pipe_name)
    pipe_name = args.pipe_name

    cwd = os.getcwd()
    os.chdir(datadir)

    obsres = dal.obsres_from_oblock_id(args.obid)

    os.chdir(cwd)
    # Direct query to insert a new task
    session = Session()
    dbtask = Task(ob_id=obsres.id)
    session.add(dbtask)
    session.commit()

    workenv = WorkEnvironment(args.basedir, datadir, dbtask)

    cwd = os.getcwd()
    os.chdir(workenv.datadir)

    recipe = dal.search_recipe_from_ob(obsres)

    # Enable intermediate results by default
    _logger.debug('enable intermediate results')
    recipe.intermediate_results = True
    _logger.debug('recipe created')

    rinput = recipe.build_recipe_input(obsres, dal)
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

    os.chdir(cwd)

    # Logging and task control
    logger_control = dict(
        logfile='processing.log',
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        enabled=True
        )

    # Load recipe control and recipe parameters from file
    task_control = dict(requirements={}, products={}, logger=logger_control)

    runinfo = {
        'taskid': dbtask.id,
        'pipeline': pipe_name,
        'recipeclass': recipe.__class__,
        'workenv': workenv,
        'recipe_version': recipe.__version__,
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

    dbtask.completion_time = datetime.datetime.now()
    session.commit()

from .ingest import metadata_fits


def mode_ingest(args, extra_args):
    print("mode ingest, path=", args.path)

    obs_blocks = {}
    frames = {}

    for x in os.walk(args.path):
        dirname, dirnames, files = x
        print('we are in', dirname)
        print('dirnames are', dirnames)
        for fname in files:
            # check based on extension
            print(fname)
            base, ext = os.path.splitext(fname)
            if ext == '.fits':
                # something
                ff = os.path.join(dirname, fname)
                result = metadata_fits(ff)
                # check
                blck_uuid = result['blckuuid']
                if blck_uuid is None:
                    continue
                if blck_uuid not in obs_blocks:
                    # new block, insert
                    ob = ObservationResult(
                        instrument=result['instrume'],
                        mode=result['obsmode']
                    )
                    ob.id = blck_uuid
                    ob.configuration = result['insconf']
                    obs_blocks[blck_uuid] = ob

                uuid_frame = result['uuid']
                if uuid_frame not in frames:
                    frames[uuid_frame] = uuid_frame
                    obs_blocks[blck_uuid].frames.append(fname)

            elif ext == '.json':
                # something else
                pass
            else:
                print("file not ingested", fname)

    from .model import MyOb, Frame, Fact
    # insert in database
    db_uri = "sqlite:///processing.db"
    #engine = create_engine(args.db_uri, echo=False)
    engine = create_engine(db_uri, echo=False)

    Session.configure(bind=engine)
    session = Session()

    for key, obs in obs_blocks.items():
        now = datetime.datetime.now()
        ob = MyOb(instrument=obs.instrument, mode=obs.mode, start_time=now)
        ob.id = obs.id
        session.add(ob)

        for name in obs.frames:
            # Insert into DB
            newframe = Frame()
            newframe.name = name
            ob.frames.append(newframe)
            #session.add(newframe)

        # Update completion time of the OB when its finished
        ob.completion_time = datetime.datetime.now()

        # Facts
        #self.add_facts(session, ob)

        session.commit()


def add_facts(session, ob):
    import numina.drps

    drps = numina.drps.get_system_drps()

    this_drp = drps.query_by_name(ob.instrument)

    tagger = None
    for mode in this_drp.modes:
        if mode.key == ob.mode:
            tagger = mode.tagger
            break

    if tagger:
        current = os.getcwd()
        os.chdir(self.datadir)
        master_tags = tagger(ob)
        os.chdir(current)

        for k, v in master_tags.items():
            fact = session.query(Fact).filter_by(key=k, value=v).first()
            if fact is None:
                fact = Fact(key=k, value=v)
            ob.facts.append(fact)


