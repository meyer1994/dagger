import datetime as dt

from prefect import flow, task, get_run_logger
from prefect_aws.s3 import


@task
def download(date: dt.date) -> str:
    logger = get_run_logger()
    logger.info('Executing download')
    return 'download'


@task
def parse(path: str) -> str:
    logger = get_run_logger()
    logger.info('Executing parse')
    return 'parse'


@task
def index(path: str) -> str:
    logger = get_run_logger()
    logger.info('Executing index')
    return 'index'


@flow
def execute() -> str:
    logger = get_run_logger()
    logger.info('Executing execute')

    date = dt.date(2023, 3, 8)
    filename = download(date)
    filename = parse(filename)
    index(filename)

    return 'execute'


if __name__ == '__main__':
    date = dt.date(2023, 3, 8)
    execute(date)
