import io
import shutil
import pandas as pd
from upath import UPath
from typing import IO

from dagster import (
    InputContext,
    OutputContext,
    UPathIOManager,
    io_manager
)


class PandasParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
        with path.open("wb") as file:
            obj.to_parquet(file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        with path.open("rb") as file:
            return pd.read_parquet(file)


class ZippedDataIOManager(UPathIOManager):
    extension: str = ".zip"

    def dump_to_path(self, context: OutputContext, obj: io.BytesIO, path: UPath):
        with path.open("wb") as file:
            shutil.copyfileobj(obj, file)

    def load_from_path(self, context: InputContext, path: UPath) -> io.BytesIO:
        with path.open("rb") as file:
            return file


@io_manager
def parquet(_):
    path = UPath('data')
    return PandasParquetIOManager(path)


@io_manager
def zipped(_):
    path = UPath('data')
    return ZippedDataIOManager(path)
