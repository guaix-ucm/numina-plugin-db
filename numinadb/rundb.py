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
from numina.user.helpers import DiskStorageDefault
from numina.dal.stored import StoredProduct
from numina.util.context import working_directory
from numina.user.clirundal import run_recipe
from numina.core.oresult import ObservationResult
import numina.store
import numina.drps

from .model import Base
from .model import Fact
from .model import Task, RecipeParameters, RecipeParameterValues
from .dal import SqliteDAL, Session
from .helpers import ProcessingTask, WorkEnvironment
from .event import call_event
from .control import mode_alias, mode_alias_del, mode_alias_list

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

    parser_alias = sub.add_parser('alias')
    # Adding alias

    parser_alias.add_argument('aliasname')

    parser_alias.add_argument('uuid', nargs='?')

    group_alias = parser_alias.add_mutually_exclusive_group()
    group_alias.add_argument('-a', action='store_const', const=mode_alias, dest='command')
    group_alias.add_argument('-d', action='store_const', const=mode_alias_del, dest='command')
    group_alias.add_argument('-l', action='store_const', const=mode_alias_list, dest='command')

    parser_alias.set_defaults(command=mode_alias)

    parser_db = sub.add_parser('db')
    # parser_run.set_defaults(command=mode_db)
    parser_db.add_argument('--initdb', nargs='?',
                           default=None,
                           const=db_default,
                           metavar='URI', 
                           help='Create a database')

    parser_db.add_argument('-c', '--task-control',
        help='insert configuration file', metavar='FILE'
    )

    parser_db.set_defaults(command=mode_db)

    parser_id = sub.add_parser('id')
    parser_id.add_argument('obid')
    parser_id.add_argument('--query',
                           help='Query')
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

    parser_ingest = sub.add_parser('ingest')
    parser_ingest.add_argument('path')

    parser_ingest.set_defaults(command=mode_ingest)

    load_entry_points()

    return parser_run


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

    if args.task_control is not None:
        print('insert task-control values from', args.task_control)
        import yaml
        with open(args.task_control) as fd:
            data = yaml.load(fd)

        res = data.get('requirements', {})
        uri = "sqlite:///processing.db"
        engine = create_engine(uri, echo=False)
        Session.configure(bind=engine)
        session = Session()


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


def create_db(uri):
    engine = create_engine(uri, echo=False)
    Base.metadata.create_all(bind=engine)


def mode_run_db(args, extra_args):
    mode_run_common_obs(args, extra_args)
    return 0


def mode_run_common_obs(args, extra_args):
    """Observing mode processing mode of numina."""

    runner = 'numina-plugin-db'
    runner_version = '1'
    engine = create_engine(args.db_uri, echo=False)
    # DAL must use the database
    if args.datadir is None:
        datadir = os.path.join(args.basedir, 'data')
    else:
        datadir = args.datadir

    dal = SqliteDAL(runner, engine, basedir=args.basedir, datadir=datadir)
    _logger.debug("DAL is %s with datadir=%s", type(dal), datadir)

    # Directories with relevant data
    _logger.debug("pipeline from CLI is %r", args.pipe_name)
    pipe_name = args.pipe_name

    with working_directory(datadir):
        obsres = dal.obsres_from_oblock_id(args.obid,
                                           override_mode=args.mode_name
                                           )

    # Direct query to insert a new task
    session = Session()
    dbtask = Task(ob_id=obsres.id)
    session.add(dbtask)
    session.commit()

    workenv = WorkEnvironment(args.basedir, datadir, dbtask)

    with working_directory(workenv.datadir):
        recipe = dal.search_recipe_from_ob(obsres)

        # Enable intermediate results by default
        _logger.debug('enable intermediate results')
        recipe.intermediate_results = True

        # Update runinfo
        _logger.debug('update recipe runinfo')
        recipe.runinfo['runner'] = runner
        recipe.runinfo['runner_version'] = runner_version
        recipe.runinfo['taskid'] = dbtask.id
        recipe.runinfo['data_dir'] = workenv.datadir
        recipe.runinfo['work_dir'] = workenv.workdir
        recipe.runinfo['results_dir'] = workenv.resultsdir
        recipe.runinfo['base_dir'] = workenv.basedir

        try:
            rinput = recipe.build_recipe_input(obsres, dal)
        except ValueError as err:
            _logger.error("during recipe input construction")
            for msg in err.args[0]:
                _logger.error(msg)
            sys.exit(0)

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
        'taskid': dbtask.id,
        'pipeline': pipe_name,
        'recipeclass': recipe.__class__,
        'workenv': workenv,
        'recipe_version': recipe.__version__,
        'runner': runner,
        'runner_version': runner_version,
        'instrument_configuration': None
    }

    task = ProcessingTask(obsres, runinfo)

    # Copy files
    if True:
        _logger.debug('copy files to work directory')
        workenv.sane_work()
        workenv.copyfiles_stage1(obsres)
        workenv.copyfiles_stage2(rinput)
        workenv.adapt_obsres(obsres)

    completed_task = run_recipe(recipe=recipe,task=task, rinput=rinput,
                                workenv=workenv, task_control=task_control)

    where = DiskStorageDefault(resultsdir=workenv.resultsdir)

    where.store(completed_task)

    dbtask.completion_time = datetime.datetime.now()
    dbtask.state = 'FINISHED'
    session.commit()


from .ingest import metadata_fits, add_ob_facts
from .ingest import metadata_json, metadata_lis
from .model import MyOb, Frame, Fact, DataProduct

def mode_ingest(args, extra_args):

    drps = numina.drps.get_system_drps()

    print("mode ingest, path=", args.path)

    obs_blocks = {}
    frames = {}
    reduction_results = {}
    ingestdir = args.path

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
                result = metadata_fits(full_fname, drps)

                # numtype
                numtype = result['type']
                blck_uuid = result['blckuuid']
                if numtype is not None:
                    # a calibration
                    print("a calibration of type", numtype)
                    reduction_uuid = result['uuid']
                    reduction_results[reduction_uuid] = (numtype, full_fname, result, True)
                    continue
                if blck_uuid is not None:
                    if blck_uuid not in obs_blocks:
                        # new block, insert
                        ob = ObservationResult(
                            instrument=result['instrument'],
                            mode=result['mode']
                        )
                        ob.id = blck_uuid
                        ob.configuration = result['insconf']

                        obs_blocks[blck_uuid] = ob

                    uuid_frame = result['uuid']
                    if uuid_frame not in frames:
                        result['path'] = fname
                        frames[uuid_frame] = result
                        obs_blocks[blck_uuid].frames.append(result)

            elif ext == '.json':
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
    db_uri = "sqlite:///processing.db"
    # engine = create_engine(args.db_uri, echo=False)
    engine = create_engine(db_uri, echo=False)

    Session.configure(bind=engine)
    session = Session()

    for key, prod in reduction_results.items():
        datatype = prod[0]
        contents = prod[1]
        metadata_basic = prod[2]
        recheck = prod[3]
        fullpath = contents
        relpath = fullpath # os.path.relpath(fullpath, self.runinfo['base_dir'])
        prod_entry = DataProduct(instrument_id=metadata_basic['instrument'],
                                 datatype=datatype,
                                 task_id=0,
                                 contents=relpath
                                 )

        if recheck:
            this_drp = drps.query_by_name(prod_entry.instrument_id)
            pipeline = this_drp.pipelines['default']
            prodtype = pipeline.load_product_from_name(prod_entry.datatype)
            # reread with correct type
            obj = numina.store.load(prodtype, prod_entry.contents)
            metadata_basic = prodtype.extract_db_info(obj)

        prod_entry.dateobs = metadata_basic['observation_date']
        prod_entry.uuid = metadata_basic['uuid']
        prod_entry.qc = metadata_basic['quality_control']
        session.add(prod_entry)

        for k, v in metadata_basic['tags'].items():
            prod_entry[k] = v

    session.commit()

    # return
    for key, obs in obs_blocks.items():
        now = datetime.datetime.now()
        ob = MyOb(instrument_id=obs.instrument, mode=obs.mode, start_time=now)
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

        query1 = session.query(Frame).join(MyOb).filter(MyOb.id == obs.id)
        res = query1.order_by(Frame.start_time).first()
        ob.start_time = res.start_time

        res = query1.order_by(Frame.completion_time.desc()).first()
        ob.completion_time = res.completion_time

        # Facts
        add_ob_facts(session, ob, ingestdir)

    # raw frames insertion
    for frame in frames:
        call_event('on_ingest_raw_fits', session, frame, frames[frame])

    session.commit()
