from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..ingest import ingest_ob_file, ingest_dir, ingest_control_file


def mode_ingest(args, extra_args, config):

    engine = create_engine(args.db_uri, echo=False)
    Session = sessionmaker(bind=engine)
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
