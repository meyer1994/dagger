import logging
import datetime as dt
from tempfile import NamedTemporaryFile

import luigi
import pangres
import pandas as pd
import sqlalchemy as sa
from luigi.format import Nop
from luigi.contrib import s3
from luigi.mock import MockTarget

import _utils


logger = logging.getLogger(__name__)


class Download(luigi.Task):
    retry_count = 3

    date = luigi.DateParameter()

    def output(self):
        path = f's3://fintz-tst-dags/luigi/{self.date}.zip'
        return s3.S3Target(path, format=Nop)

    def run(self):
        outp = self.output()
        client = s3.S3Client()
        client.copy('s3://fintz-tst-dags/out.zip', outp.path)


class Parse(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return Download(date=self.date)

    def output(self):
        path = f's3://fintz-tst-dags/luigi/{self.date}.parquet'
        return s3.S3Target(path, format=Nop)

    def run(self):
        client = s3.S3Client()

        inpt = self.input()
        outp = self.output()

        with NamedTemporaryFile(suffix='.zip') as tmp:
            client.get(inpt.path, tmp.name)
            df = _utils.read_df(tmp.name)

        df.to_parquet(outp.path)


class Index(luigi.Task):
    date = luigi.DateParameter()

    # Adapted from:
    #   https://stackoverflow.com/a/50552458
    task_complete = False

    def complete(self):
        return self.task_complete

    def requires(self):
        return Parse(date=self.date)

    def run(self):
        inpt = self.input()

        df = pd.read_parquet(inpt.path)

        host = 'postgresql://postgres@localhost:5432/postgres'
        engine = sa.create_engine(host)

        with engine.connect() as conn:
            chunks = pangres.upsert(
                df=df,
                con=conn,
                table_name='b3_acoes',
                if_row_exists='ignore',
                chunksize=10_000,
                yield_chunks=True
            )

            for item in chunks:
                logger.info('Inserted %d rows', item.rowcount)

        self.task_complete = True
