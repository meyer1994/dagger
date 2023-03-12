import io

import pangres
import pandas as pd
from upath import UPath
from dagster import asset, AssetIn, RetryPolicy, DailyPartitionsDefinition

from test import _utils


CONFIG_DOWNLOAD = {
    'io_manager_key': 'zipped',
    'retry_policy': RetryPolicy(max_retries=3, delay=300),
    'partitions_def': DailyPartitionsDefinition(start_date='2023-03-01'),
}

CONFIG_PARSE = {
    'io_manager_key': 'parquet',
    'ins': {'data': AssetIn('download')},
    'partitions_def': DailyPartitionsDefinition(start_date='2023-03-01'),
}

CONFIG_INDEX = {
    'ins': {'df': AssetIn('parse')},
    'required_resource_keys': {'pg'},
    'retry_policy': RetryPolicy(max_retries=3, delay=300),
    'partitions_def': DailyPartitionsDefinition(start_date='2023-03-01'),
}


@asset(**CONFIG_DOWNLOAD)
def download(context) -> bytes:
    path = UPath('s3://fintz-tst-dags/out.zip')
    return path.read_bytes()


@asset(**CONFIG_PARSE)
def parse(context, data: bytes) -> pd.DataFrame:
    data = io.BytesIO(data)
    return _utils.read_df(data)


@asset(**CONFIG_INDEX)
def index(context, df: pd.DataFrame) -> None:
    chunks = pangres.upsert(
        df=df,
        con=context.resources.pg,
        table_name='b3_acoes',
        if_row_exists='ignore',
        chunksize=10_000,
        yield_chunks=True
    )

    for item in chunks:
        context.log.info('Inserted %d rows', item.rowcount)
