#
# Copyright 2016-2017 Universidad Complutense de Madrid
#
# This file is part of Numina DB
#
# SPDX-License-Identifier: GPL-3.0+
# License-Filename: LICENSE.txt
#

from __future__ import print_function

from sqlalchemy import create_engine

from .dal import Session
from .model import ObservingBlockAlias

def mode_alias(args, extra_args):
    uri = "sqlite:///processing.db"
    engine = create_engine(uri, echo=False)
    Session.configure(bind=engine)
    session = Session()
    dbpar = session.query(ObservingBlockAlias).filter_by(alias=args.aliasname).first()
    force = False
    if dbpar:
        if force:
            dbpar.uuid = args.uuid
        else:
            print('alias already exists')
    else:
        newalias = ObservingBlockAlias()
        newalias.uuid = args.uuid
        newalias.alias = args.aliasname
        session.add(newalias)
    session.commit()


def mode_alias_del(args, extra_args):
    uri = "sqlite:///processing.db"
    engine = create_engine(uri, echo=False)
    Session.configure(bind=engine)
    session = Session()
    dbpar = session.query(ObservingBlockAlias).filter_by(alias=args.aliasname)
    cas = dbpar.delete()
    if cas == 0:
        print('nothing was deleted')
    else:
        print(args.aliasname, 'was deleted')
    session.commit()


def mode_alias_list(args, extra_args):
    uri = "sqlite:///processing.db"
    engine = create_engine(uri, echo=False)
    Session.configure(bind=engine)
    session = Session()
    dbpar = session.query(ObservingBlockAlias).filter_by(alias=args.aliasname)
    for res in dbpar:
        print('alias:', res.alias, 'uuid:', res.uuid)
