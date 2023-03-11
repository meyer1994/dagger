# Dagger

Small project to test ETL pipelines

## Luigi

```
cd luigi
PYTHONPATH='.' luigi \
    --local-scheduler \
    --module 'main' \
    'Index' \
    --date 2023-03-08
```

