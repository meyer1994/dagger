import pandas as pd
from upath import UPath

from dagster import (
    InputContext,
    OutputContext,
    UPathIOManager,
    io_manager
)


class PandasParquetIOManager(UPathIOManager):
    extension: str = '.parquet'

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
        with path.open('wb') as file:
            obj.to_parquet(file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        with path.open('rb') as file:
            return pd.read_parquet(file)


class ZippedDataIOManager(UPathIOManager):
    extension: str = '.zip'

    def dump_to_path(self, context: OutputContext, obj: bytes, path: UPath):
        with path.open('wb') as file:
            file.write(obj)

    def load_from_path(self, context: InputContext, path: UPath) -> bytes:
        with path.open('rb') as file:
            return file.read()


@io_manager
def parquet(_):
    path = UPath('s3://fintz-tst-dags/dagster')
    return PandasParquetIOManager(path)


@io_manager
def zipped(_):
    path = UPath('s3://fintz-tst-dags/dagster')
    return ZippedDataIOManager(path)
