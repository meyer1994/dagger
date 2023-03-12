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

Deploy your code:

```sh
prefect deployment build main.py:daily \
    --name test-daily \
    --cron '0 23 * * 1-5' \
    --timezone 'America/Sao_Paulo'
prefect deployment apply daily-deployment.yaml
```

Start the agent:

```sh
prefect agent start -p 'default-agent-pool'
```
