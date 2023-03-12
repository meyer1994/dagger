from dagster import Definitions, define_asset_job, ScheduleDefinition
from dagster_aws.s3 import s3_resource

from b3_acoes import assets, resources, iomanagers


# job = define_asset_job(name='b3_acoes_daily_1am', selection='*')

# More info here:
#   https://crontab.pro/#0_1_*_*_2-6
# schedule = ScheduleDefinition(job=job, cron_schedule='0 1 * * 2-6')

defs = Definitions(
    assets=[assets.download],
    # schedules=[schedule],
    resources={
        's3': s3_resource,
        'pg': resources.pg,
        'parquet': iomanagers.parquet,
        'zipped': iomanagers.zipped,
    },
)
