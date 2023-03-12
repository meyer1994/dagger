from dagster import Definitions, define_asset_job, ScheduleDefinition

from b3_acoes import assets, resources, iomanagers


job = define_asset_job(name='b3_acoes_daily_1am', selection='*')

# More info here:
#   https://crontab.pro/#0_1_*_*_2-6
schedule = ScheduleDefinition(job=job, cron_schedule='0 1 * * 2-6')

defs = Definitions(
    assets=[assets.download, assets.parse, assets.index],
    schedules=[schedule],
    resources={
        'b3': resources.b3,
        'pg': resources.pg,
        'parquet': iomanagers.parquet,
        'zipped': iomanagers.zipped,
    },
)
