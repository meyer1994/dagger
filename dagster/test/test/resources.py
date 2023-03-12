import os
from contextlib import contextmanager

import sqlalchemy as sa
from dagster import resource, StringSource


@resource
@contextmanager
def pg(context):
    default = 'postgresql://postgres@localhost:5432/postgres'
    host = os.getenv('POSTGRES_HOST', default)

    engine = sa.create_engine(host)
    with engine.connect() as conn:
        yield conn
