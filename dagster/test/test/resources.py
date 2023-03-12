from contextlib import contextmanager

import sqlalchemy as sa
from dagster import resource


@resource
@contextmanager
def pg(init_context):
    engine = sa.create_engine('postgresql://postgres@localhost:5432/postgres')
    with engine.connect() as conn:
        yield conn
