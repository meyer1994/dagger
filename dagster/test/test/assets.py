from dagster import asset

@asset
def download(context) -> str:
    context.log.info('Executing download')
    return 'download'
