import io
import math
import zipfile
import datetime as dt
from pathlib import Path
from typing import IO

import pangres
import pandas as pd
from dagster import (
    asset,
    AssetIn,
    Output,
    MetadataValue,
    RetryPolicy,
    DataVersion,
    DailyPartitionsDefinition,
)

from . import _utils


CONFIG_DOWNLOAD = {
    'required_resource_keys': {'b3'},
    'retry_policy': RetryPolicy(max_retries=3, delay=300),
    'output_required': False,
    'io_manager_key': 'zipped',
}

CONFIG_PARSE = {
    'ins': {'buffer': AssetIn('download')},
    'io_manager_key': 'parquet',
}

CONFIG_INDEX = {
    'ins': {'df': AssetIn('parse')},
    'required_resource_keys': {'pg'},
    'retry_policy': RetryPolicy(max_retries=2, delay=300),
}


@asset(**CONFIG_DOWNLOAD)
def download(context) -> Output[io.BytesIO]:
    date_iso = context.asset_partition_key_for_output()
    date = dt.date.fromisoformat(date_iso)

    # path = context.resources.b3.fetch(date_iso)
    data = Path('out.zip').read_bytes()
    data = io.BytesIO(data)

    size = data.getbuffer().nbytes

    version = DataVersion(date_iso)
    metadata = {'file_size': size, 'date': date_iso}
    return Output(value=data, metadata=metadata, data_version=version)


@asset(**CONFIG_PARSE)
def parse(context, buffer: io.BufferedReader) -> Output[pd.DataFrame]:
    df = _utils.read(buffer)

    markdown = df.head().reset_index().to_markdown()
    metadata = {'total_rows': len(df), 'head': MetadataValue.md(markdown)}
    return Output(value=df, metadata=metadata)


@asset(**CONFIG_INDEX)
def index(context, df: pd.DataFrame) -> Output[None]:
    chunks = math.ceil(len(df) / 10_000)
    context.log.info('Upserting %d rows in %d chunks', len(df), chunks)

    iterator = pangres.upsert(
        df=df,
        con=context.resources.pg,
        table_name='b3_acoes',
        if_row_exists='ignore',
        chunksize=10_000,
        yield_chunks=True,
    )

    total = 0
    for i, result in enumerate(iterator, start=1):
        context.log.info('Inserted chunk %02d of %02d', i, chunks)
        total += result.rowcount

    metadata = {'total_rows': len(df), 'rows_inserted': total}
    return Output(value=None, metadata=metadata)
