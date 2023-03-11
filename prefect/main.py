import datetime as dt
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from prefect import flow, task
from prefect_shell import ShellOperation
from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver

from constants import (
    URL,
    DF_NAMES,
    DF_COLSPECS,
    TABLE_NAME,
    HOST,
    QUERY_COPY,
    QUERY_TABLE,
    QUERY_CLEAN
)


@flow(retries=3)
def download(date: dt.date) -> Path:
    date_url = date.strftime('%d%m%Y')
    date_iso = date.isoformat()

    url = URL % date_url

    command = f'curl {url} --output {date_iso}.zip --silent'
    ShellOperation(commands=[command]).run()

    return Path(f'{date_iso}.zip')


@task
def _remove_nulls(df: pd.DataFrame):
    mapper = lambda i: i.replace('\0', '')
    return df.applymap(mapper, na_action='ignore')


@task
def _remove_whitespace(df: pd.DataFrame):
    df = df.applymap(lambda i: i.strip(), na_action='ignore')
    df = df.applymap(lambda i: i.split(), na_action='ignore')
    df = df.applymap(lambda i: ' '.join(i), na_action='ignore')
    return df


@task
def _parse_floats(df: pd.DataFrame):
    floats = [
        'preabe',
        'premax',
        'premin',
        'premed',
        'preult',
        'preofc',
        'preofv',
        'voltot',
        'preexe',
        'ptoexe',
    ]
    mapper = lambda i: i.replace(',', '.')
    df[floats] = df[floats].applymap(mapper, na_action='ignore')
    df[floats] = df[floats].astype('double')
    df[floats] /= 100
    return df


@task
def _parse_ints(df: pd.DataFrame):
    ints = ['quatot']
    df[ints] = df[ints].astype('Int64')
    return df


@task
def _parse_dates(df: pd.DataFrame):
    dates = ['dtpreg', 'datven']
    mapper = lambda i: dt.datetime.strptime(i, '%Y%m%d').date()
    df[dates] = df[dates].applymap(mapper, na_action='ignore')
    return df


@flow
def parse(path: Path) -> Path:
    df = pd.read_fwf(
        path,
        encoding='latin',
        dtype='string',
        compression='zip',
        colspecs=DF_COLSPECS,
        names=DF_NAMES
    )

    # Delete first and last lines
    df.drop(df.head(1).index, inplace=True)
    df.drop(df.tail(1).index, inplace=True)

    # Parse everything
    df = _remove_nulls(df)
    df = _remove_whitespace(df)
    df = _parse_ints(df)
    df = _parse_floats(df)
    df = _parse_dates(df)

    # Save
    path = Path(f'{path.stem}.csv')
    df.to_csv(path, index=False)

    return path


@flow
def index(date: dt.date, filename: Path):
    credentials = DatabaseCredentials(url=HOST)
    engine = credentials.get_engine()

    date = date.isoformat()

    # Adapted from:
    #   https://stackoverflow.com/a/34523707
    conn = engine.raw_connection()

    with conn.cursor() as cur:
        cur.execute(QUERY_TABLE)
        with open(filename, 'rt') as f:
            cur.copy_expert(QUERY_COPY, f)
        cur.execute(QUERY_CLEAN % (TABLE_NAME, date))

    conn.commit()


@flow
def execute(date: dt.date):
    filename = download(date)
    filename = parse(filename)
    index(date, filename)


if __name__ == '__main__':
    date = dt.date.today() - dt.timedelta(days=1)
    execute(date)
