import logging

from numina.util.context import working_directory
from numina.user.helpers import WorkEnvironment
from numina.user.baserun import run_recipe_timed

from ..helpers import ProcessingTask

_logger = logging.getLogger(__name__)

runner = 'numina-plugin-db'
runner_version = '1'


class DiskStorageDefault:
    pass


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

    completed_task = run_recipe_timed(recipe=recipe, task=task, rinput=rinput,
                                      workenv=workenv, task_control=task_control)

    where = DiskStorageDefault(resultsdir=workenv.resultsdir)
    where.task = 'task.json'
    where.result = 'result.json'

    result = where.store(completed_task)
    return result


def reduction(**kwargs):
    print('reduction', kwargs)
    return 'something'
