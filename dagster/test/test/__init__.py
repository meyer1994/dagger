from dagster import Definitions, asset, op, job, asset_sensor, AssetKey, RunRequest


@asset
def download(context) -> str:
    context.log.info('Executing download')
    return 'download'


@op
def parse(context, data: str) -> str:
    context.log.info('Executing parse')
    return 'parse'

@op
def index(context, data: str) -> str:
    context.log.info('Executing index')
    return 'index'


@job
def execute():
    data = download()
    data = parse(data)
    index(data)


@asset_sensor(asset_key=AssetKey('download'), job=execute)
def sensor(context, asset_event):
    yield RunRequest()


defs = Definitions(
    assets=[download],
    jobs=[execute],
    sensors=[sensor],
)
