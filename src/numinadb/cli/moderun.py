import datetime
import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..dal import SqliteDAL, search_oblock_from_id
from ..model import DataProcessingTask
from .methods import reduction, reductionOB

_logger = logging.getLogger("numina.db")

methods = dict()
methods['reductionOB'] = reductionOB
methods['reduction'] = reduction

runner = 'numina-plugin-db'
runner_version = '1'


def mode_run_db(args, extra_args, config):
    mode_run_common_obs(args, extra_args)
    return 0


def mode_run_common_obs(args, extra_args, config):
    """Observing mode processing mode of numina."""

    engine = create_engine(args.db_uri, echo=False)
    Session = sessionmaker(bind=engine)
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
