import datetime as dt
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile

import pangres
import pandas as pd
import sqlalchemy as sa
from prefect import flow, task, get_run_logger
from prefect_aws.s3 import S3Bucket
from prefect.blocks.system import Secret

import _utils


@task
def download(date: dt.date) -> str:
    s3 = S3Bucket.load('aws-s3-bucket')

    filename = date.isoformat()
    filename = f'prefect/{date}.zip'

    with NamedTemporaryFile(suffix='.zip') as tmp:
        s3.download_object_to_path(from_path='out.zip', to_path=tmp.name)
        s3.upload_from_path(from_path=tmp.name, to_path=filename)

    return filename


@task
def parse(date: dt.date, path: str) -> str:
    s3 = S3Bucket.load('aws-s3-bucket')

    filename = date.isoformat()
    filename = f'prefect/{filename}.parquet'

    with NamedTemporaryFile(suffix='.zip') as tmp:
        s3.download_object_to_path(from_path=path, to_path=tmp.name)
        df = _utils.read_df(tmp.name)

    with NamedTemporaryFile(suffix='.parquet') as tmp:
        df.to_parquet(tmp.name)
        s3.upload_from_path(from_path=tmp.name, to_path=filename)

    return filename


@task
def index(date: dt.date, path: str):
    logger = get_run_logger()

    s3 = S3Bucket.load('aws-s3-bucket')
    host = Secret.load("aws-rds-postgres-host").get()

    with NamedTemporaryFile(suffix='.parquet') as tmp:
        s3.download_object_to_path(from_path=path, to_path=tmp.name)
        df = pd.read_parquet(tmp.name)

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


@flow
def daily():
    date = dt.date.today()
    filename = download(date)
    filename = parse(date, filename)
    index(date, filename)


@flow
def execute(date: dt.date):
    filename = download(date)
    filename = parse(date, filename)
    index(date, filename)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('date', type=dt.date.fromisoformat)
    args = parser.parse_args()
    execute(args.date)
