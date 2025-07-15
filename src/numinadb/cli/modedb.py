
from sqlalchemy import create_engine

from ..base import Base


def mode_db(args, extra_args, config):

    if args.initdb is not None:
        print(f"Create new database in {args.initdb}")
        create_db(uri=args.initdb)


def create_db(uri):
    engine = create_engine(uri, echo=False)
    Base.metadata.create_all(bind=engine)
