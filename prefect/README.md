# Prefect

## Deployment

Set the correct workspace:

```
prefect cloud workspace set --workspace meyer1994/test
```

Deploy your code:

```sh
prefect deployment build main.py:execute --name test
prefect deployment apply execute-deployment.yaml
```

Start the agent:

```sh
prefect agent start -p 'default-agent-pool'
```
