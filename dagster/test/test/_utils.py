import datetime as dt

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

DF_INTS = [
    'quatot'
]

DF_FLOATS = [
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

DF_DATES = [
    'dtpreg',
    'datven'
]

DF_INDEX = [
    'nomres',
    'codneg',
    'dtpreg',
    'tipreg',
    'codbdi',
    'prazot'
]


def _remove_nulls(df: pd.DataFrame) -> pd.DataFrame:
    mapper = lambda i: i.replace('\0', '')
    return df.applymap(mapper, na_action='ignore')


def _remove_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.applymap(lambda i: i.strip(), na_action='ignore')
    df = df.applymap(lambda i: i.split(), na_action='ignore')
    df = df.applymap(lambda i: ' '.join(i), na_action='ignore')
    return df


def _parse_floats(df: pd.DataFrame) -> pd.DataFrame:
    mapper = lambda i: i.replace(',', '.')
    df[DF_FLOATS] = df[DF_FLOATS].applymap(mapper, na_action='ignore')
    df[DF_FLOATS] = df[DF_FLOATS].astype('double')
    df[DF_FLOATS] /= 100
    return df


def _parse_ints(df: pd.DataFrame) -> pd.DataFrame:
    df[DF_INTS] = df[DF_INTS].astype('Int64')
    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    mapper = lambda i: dt.datetime.strptime(i, '%Y%m%d').date()
    df[DF_DATES] = df[DF_DATES].applymap(mapper, na_action='ignore')
    return df


def _set_indexes(df: pd.DataFrame) -> pd.DataFrame:
    df['prazot'].fillna('', inplace=True)  # has some null values
    df.set_index(DF_INDEX, inplace=True, verify_integrity=True)
    return df


def read_df(filename: str) -> pd.DataFrame:
    df = pd.read_fwf(
        filename,
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
    df = _set_indexes(df)

    return df
