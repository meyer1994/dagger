import datetime as dt
from pathlib import Path

import pandas as pd

DF_COLSPECS = [
    (0, 2),
    (2, 10),
    (10, 12),
    (12, 24),
    (24, 27),
    (27, 39),
    (39, 49),
    (49, 52),
    (52, 56),
    (56, 69),
    (69, 82),
    (82, 95),
    (95, 108),
    (108, 121),
    (121, 134),
    (134, 147),
    (147, 152),
    (152, 170),
    (170, 188),
    (188, 201),
    (201, 202),
    (202, 210),
    (210, 217),
    (217, 230),
    (230, 242),
    (242, 245),
]

DF_NAMES = [
    'tipreg',
    'dtpreg',
    'codbdi',
    'codneg',
    'tpmerc',
    'nomres',
    'especi',
    'prazot',
    'modref',
    'preabe',
    'premax',
    'premin',
    'premed',
    'preult',
    'preofc',
    'preofv',
    'totneg',
    'quatot',
    'voltot',
    'preexe',
    'indopc',
    'datven',
    'fatcot',
    'ptoexe',
    'codisi',
    'dismes',
]


def _remove_nulls(df: pd.DataFrame):
    mapper = lambda i: i.replace('\0', '')
    return df.applymap(mapper, na_action='ignore')


def _remove_whitespace(df: pd.DataFrame):
    df = df.applymap(lambda i: i.strip(), na_action='ignore')
    df = df.applymap(lambda i: i.split(), na_action='ignore')
    df = df.applymap(lambda i: ' '.join(i), na_action='ignore')
    return df


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


def _parse_ints(df: pd.DataFrame):
    ints = ['quatot']
    df[ints] = df[ints].astype('Int64')
    return df


def _parse_dates(df: pd.DataFrame):
    dates = ['dtpreg', 'datven']
    mapper = lambda i: dt.datetime.strptime(i, '%Y%m%d').date()
    df[dates] = df[dates].applymap(mapper, na_action='ignore')
    return df


def read(path: Path) -> pd.DataFrame:
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

    # Remove nulls from primary key
    df['prazot'].fillna('', inplace=True)

    # Index
    index = ['nomres', 'codneg', 'dtpreg', 'tipreg', 'codbdi', 'prazot']
    df.set_index(index, inplace=True, verify_integrity=True)

    return df
