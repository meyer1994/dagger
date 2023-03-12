import io
import datetime as dt
from contextlib import contextmanager

import httpx
import sqlalchemy as sa
from dagster import resource


class B3Client:
    URL = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%s.zip'

    def fetch(self, date: dt.date) -> io.BytesIO:
        date_url = date.strftime('%d%m%Y')

        url = self.URL % date_url

        buffer = io.BytesIO()
        with httpx.stream('GET', url, timeout=3_600) as inpt:
            for data in inpt.iter_bytes():
                buffer.write(data)
        buffer.seek(0)

        return buffer


@resource
def b3(init_context) -> B3Client:
    return B3Client()


@resource
@contextmanager
def pg(init_context):
    engine = sa.create_engine('postgresql://postgres@localhost:5432/postgres')
    with engine.connect() as conn:
        yield conn
