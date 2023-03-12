
URL = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%s.zip'

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

TABLE_NAME = 'b3_acoes'

HOST = 'postgresql://postgres@localhost:5432/postgres'

QUERY_TABLE = r'''
    CREATE TABLE IF NOT EXISTS b3_acoes (
        tipreg  text,
        dtpreg  date,
        codbdi  text,
        codneg  text,
        tpmerc  text,
        nomres  text,
        especi  text,
        prazot  text,
        modref  text,
        preabe  double precision,
        premax  double precision,
        premin  double precision,
        premed  double precision,
        preult  double precision,
        preofc  double precision,
        preofv  double precision,
        totneg  bigint,
        quatot  bigint,
        voltot  double precision,
        preexe  double precision,
        indopc  text,
        datven  date,
        fatcot  text,
        ptoexe  double precision,
        codisi  text,
        dismes  text
    );
'''

QUERY_COPY = r'''
    COPY b3_acoes (
        tipreg,
        dtpreg,
        codbdi,
        codneg,
        tpmerc,
        nomres,
        especi,
        prazot,
        modref,
        preabe,
        premax,
        premin,
        premed,
        preult,
        preofc,
        preofv,
        totneg,
        quatot,
        voltot,
        preexe,
        indopc,
        datven,
        fatcot,
        ptoexe,
        codisi,
        dismes
    )
    FROM
        STDIN
    WITH
        (FORMAT CSV, HEADER TRUE)
'''

QUERY_CLEAN = '''
    DELETE FROM
        %s
    WHERE
        dtpreg
    LIKE '%s%%'
'''
