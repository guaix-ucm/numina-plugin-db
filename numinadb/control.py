#
# Copyright 2016-2017 Universidad Complutense de Madrid
#
# This file is part of Numina DB
#
# SPDX-License-Identifier: GPL-3.0+
# License-Filename: LICENSE.txt
#

from __future__ import print_function


from .model import ObservingBlockAlias


def mode_alias_add(session, aliasname, uuid, force=False):

    dbpar = session.query(ObservingBlockAlias).filter_by(alias=aliasname).first()

    if dbpar:
        if force:
            dbpar.uuid = uuid
        else:
            print('alias already exists')
    else:
        newalias = ObservingBlockAlias()
        newalias.uuid = uuid
        newalias.alias = aliasname
        session.add(newalias)
    session.commit()


def mode_alias_del(session, aliasname):

    dbpar = session.query(ObservingBlockAlias).filter_by(alias=aliasname)
    cas = dbpar.delete()
    if cas == 0:
        print('nothing was deleted')
    else:
        print(aliasname, 'was deleted')
    session.commit()


def mode_alias_list(session):
    dbpar = session.query(ObservingBlockAlias)
    for res in dbpar:
        print('alias:', res.alias, 'uuid:', res.uuid)
