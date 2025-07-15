#
# Copyright 2016-2025 Universidad Complutense de Madrid
#
# This file is part of Numina DB
#
# SPDX-License-Identifier: GPL-3.0-or-later
# License-Filename: LICENSE.txt
#

"""User command line interface of Numina DB"""


import logging
import os

from .modealias import mode_alias
from .modedb import mode_db
from .moderun import mode_run_db
from .modeingest import mode_ingest


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

    return parser_rundb
