
# Dagger

Small project to test ETL pipelines

## About

This is a repo contains the same Extract Transform and Load (ETL) pipeline
implemented using three different frameworks services:

- [Luigi](#luigi)
- [Prefect](#prefetc)
- [Dagster](#dagster)

### Pipeline

The implemented pipeline is very simple. We download the data from a source,
parse it and insert it into a database:

```mermaid
graph LR;
Download --> Parse;
Parse --> Index;
```

The job should be able to receive a parameter, a `date` object, which is used
to download the correct file from the web.

## Luigi

[Luigi][1] is the simplest one to use. It works as a distributed makefile where
you create inputs and outputs for each task. We define tasks, `luigi.Task` that
have an input and an output.

Luigi works this way, it first creates the dependency graph between the tasks.
From the starting task, it will check if its input is already processed. For
luigi, "processed" means it exists. So, if the input is already there, in your
local storage or in S3, it will be considered done.

This has a few problems, if the input changes, but it has the same name, in
case of files, it won't be processed because it is considered done. This can
be fixed with some extra code though.

To run the luigi ETL pipeline simply do the following:

```sh
cd luigi
PYTHONPATH='.' luigi \
    --local-scheduler \
    --module 'main' \
    'Index' \
    --date 2023-03-08
```

## Prefect



## Dagster


[1]: https://github.com/spotify/luigi
