import math
import datetime as dt
from tempfile import NamedTemporaryFile

import pangres
import pandas as pd
import sqlalchemy as sa
from prefect.blocks.system import Secret
from prefect import flow, task, get_run_logger
from prefect_aws.s3 import S3Bucket

import _utils


@task
def download(date: dt.date) -> str:
    tmp = NamedTemporaryFile(suffix='.zip')

    s3 = S3Bucket.load('fintz-tst-dags-aws-s3')
    s3.download_object_to_path(from_path='out.zip', to_path=tmp.name)

    date = date.isoformat()
    filename = f'prefect/{date}.zip'

    return s3.upload_from_path(from_path=tmp.name, to_path=filename)


@task
def parse(path: str) -> str:
    tmp = NamedTemporaryFile(suffix='.zip')

    s3 = S3Bucket.load('fintz-tst-dags-aws-s3')
    s3.download_object_to_path(from_path=path, to_path=tmp.name)

    df = _utils.read(tmp.name)
    df.to_parquet(tmp.name)

    filename = path.removesuffix('.zip')
    filename = f'{filename}.parquet'

    return s3.upload_from_path(from_path=tmp.name, to_path=filename)


@task
def index(path: str) -> str:
    logger = get_run_logger()

    tmp = NamedTemporaryFile(suffix='.zip')

    s3 = S3Bucket.load('fintz-tst-dags-aws-s3')
    s3.download_object_to_path(from_path=path, to_path=tmp.name)

    df = pd.read_parquet(tmp.name)

    chunks = math.ceil(len(df) / 10_000)
    logger.info('Upserting %d rows in %d chunks', len(df), chunks)

    host = Secret.load('fintz-tst-dags-psql')
    host = host.get()

    engine = sa.create_engine(host)

    with engine.connect() as conn:
        iterator = pangres.upsert(
            df=df,
            con=conn,
            table_name='b3_acoes',
            if_row_exists='ignore',
            chunksize=10_000,
            yield_chunks=True,
        )

        total = 0
        for i, result in enumerate(iterator, start=1):
            logger.info('Inserted chunk %02d of %02d', i, chunks)
            total += result.rowcount


@flow
def execute(date: dt.date) -> str:
    date = dt.date(2023, 3, 8)
    filename = download(date)
    filename = parse(filename)
    index(filename)


if __name__ == '__main__':
    date = dt.date(2023, 3, 8)
    execute(date)
