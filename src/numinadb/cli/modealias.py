
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..control import mode_alias_add, mode_alias_del, mode_alias_list


def mode_alias(args, extra_args, config):

    engine = create_engine(args.db_uri, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    if args.action == 'add':
        mode_alias_add(session, args.aliasname, args.uuid, force=args.force)

    elif args.action == 'del':
        mode_alias_del(session, args.aliasname)

    elif args.action == 'list':
        mode_alias_list(session)
    else:
        pass
