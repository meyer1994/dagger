# Prefect

## Setup

```sh
pip install -r requirements.txt
```

## Configuration

This example code assumes you have setup proper AWS, S3 and a secret block with
the following names:

- 'aws-credentials'
- 'aws-s3-bucket'
- 'aws-rds-postgres-host'

## Deployment

Set the correct workspace:

```
prefect cloud workspace set --workspace meyer1994/test
```

The following code will deploy the a flow that will run every day at 23h,
Brazil time.

```sh
prefect deployment build main.py:daily \
    --name test-daily \
    --cron '0 23 * * 1-5' \
    --timezone 'America/Sao_Paulo'
prefect deployment apply daily-deployment.yaml
```

The following code will deploy a function that receives a date as parameter.
This way, we can, using the UI or the CLI, execute the flow for a date. Good
for quick fixes when needed.

```sh
prefect deployment build main.py:execute --name test-execute
prefect deployment apply execute-deployment.yaml
```

To execute the flow above you can do it via the UI or using the following:

```sh
prefect deployment run execute/test-execute --param 'date=2023-03-11'
```

Start the agent:

```sh
prefect agent start -p 'default-agent-pool'
```
