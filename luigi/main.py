import csv
import datetime as dt

import luigi
import pandas as pd
from luigi.contrib import postgres
from luigi.contrib import external_program

from constants import URL, DF_NAMES, DF_COLSPECS, TABLE_NAME, TABLE_COLUMNS


class Download(external_program.ExternalProgramTask):
    retry_count = 3

    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(f'{self.date}.zip')

    def program_args(self):
        url = URL % f'{self.date.day:02d}{self.date.month:02d}{self.date.year}'
        filename = self.output().path
        return ['curl', url, '--output', filename, '--silent']


class Parse(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return Download(date=self.date)

    def output(self):
        return luigi.LocalTarget(f'{self.date}.csv')

    def run(self):
        inpt = self.input()

        df = pd.read_fwf(
            inpt.path,
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
        df = self._remove_nulls(df)
        df = self._remove_whitespace(df)
        df = self._parse_ints(df)
        df = self._parse_floats(df)
        df = self._parse_dates(df)

        out = self.output()
        df.to_csv(out.path, index=False)

    def _remove_nulls(self, df: pd.DataFrame):
        mapper = lambda i: i.replace('\0', '')
        return df.applymap(mapper, na_action='ignore')

    def _remove_whitespace(self, df: pd.DataFrame):
        df = df.applymap(lambda i: i.strip(), na_action='ignore')
        df = df.applymap(lambda i: i.split(), na_action='ignore')
        df = df.applymap(lambda i: ' '.join(i), na_action='ignore')
        return df

    def _parse_floats(self, df: pd.DataFrame):
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

    def _parse_ints(self, df: pd.DataFrame):
        ints = ['quatot']
        df[ints] = df[ints].astype('Int64')
        return df

    def _parse_dates(self, df: pd.DataFrame):
        dates = ['dtpreg', 'datven']
        mapper = lambda i: dt.datetime.strptime(i, '%Y%m%d').date()
        df[dates] = df[dates].applymap(mapper, na_action='ignore')
        return df



class Index(postgres.CopyToTable):
    host = 'localhost'
    database = 'postgres'
    user = 'postgres'
    password = ''
    table = TABLE_NAME
    columns = TABLE_COLUMNS
    port = '5432'

    date = luigi.DateParameter()

    def requires(self):
        return Parse(date=self.date)

    def rows(self):
        with self.input().open() as f:
            reader = csv.reader(f)
            next(reader)
            yield from reader

    def init_copy(self, conn):
        query = f"DELETE FROM {self.table} WHERE dtpreg LIKE '{self.date}%'"
        with conn.cursor() as cur:
            cur.execute(query)
