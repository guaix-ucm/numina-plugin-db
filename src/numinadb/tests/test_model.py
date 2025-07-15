import pytest
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

from ..model import Base


@pytest.fixture
def session():
    database = "sqlite:///:memory:"
    engine = create_engine(database, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    try:
        with Session() as session:
            yield session
    finally:
        Base.metadata.drop_all(engine)


def test_model(session):
    """Test expected tables are created"""
    expected_tables = ['data_obs_fact', 'obs', 'instruments', 'fact', 'dp_task',
                       'frames', 'obs_alias', 'parameter_facts',
                       'recipe_parameter_values', 'recipe_parameters',
                       'product_facts', 'products', 'reduction_result_values',
                       'reduction_results']
    metadata = MetaData()
    metadata.reflect(bind=session.get_bind())
    tables = list(metadata.tables.keys())
    assert sorted(tables) == sorted(expected_tables)
