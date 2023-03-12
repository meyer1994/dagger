# Dagger

Small project to test ETL pipelines

## About

This is a repo contains the same Extract Transform and Load (ETL) pipeline
implemented using three different frameworks/services:

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

Luigi is the simplest one. You define tasks that have input and outputs. If you
have 2 tasks, like so:

```mermaid
graph LR:
    Task#1 --> Task#2
```

If the output of Task#2 exists, Luigi assumes that Task#2 is already processed
and it will use the existing output as input to Task#1.

> :warning: Luigi only checks for existent of the output. It does not do any
> other checks on the data itself.

We can pass parameters to the pipeline. Like so:

```sh
luigi --module 'main' 'Index' --date '2023-03-08'
```

You can define inputs and outputs that take the value of the parameter to check
if the output already exists. To use the parameter on all steps of the pipeline
you will need to define the parameter for all tasks. Example:

```py
import luigi

class Task1(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return Task2(date=self.date)

class Task2(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(f'{self.date}.csv')
```

### Pros

- Very simple to use
- Many integrations (S3, PostgreSQL, etc.)
- Easy to pass parameters through tasks
- We need to host ourselves (cheaper)

### Cons

- No scheduler
- Bad visibility of task runs/flows
- We need to host ourselves (time consuming)

## Prefect

Prefect works in a similar way of Luigi. However, it has many more features. It
allows you to abstract distributed tasks by simply writing python code. For
example:

```py
from prefect import flow, task

@task
def first_task():
    return 123

@flow
def my_flow():
    data = first_task()
    data = second_task(data)
```

The `@task` function is a Prefect task and it can be called many times in
parallel by other `@flow` tasks. It also include lots of logging. It logs the
input and output of each task/flow allowing us to follow the code execution
very easily.

It also allows you to make tasks cacheable or even persist its outputs. This
can be configured in a transparent way, so the main logic code, as in example
above, does not need to be changed whatsoever.

### Pros

- Nice UI
- Nice task/flow abstraction
- Lots of integrations (AWS, Azure, etc.)
- Distributed configuration
- Lots of visibility
- Scheduler built in
- More complex than Luigi

### Cons

- More complicated than Luigi
- Only self hosted (executors)

## Dagster

Dagster can be used in a similar way to Prefect. However, Dagster implements
a new abstraction called Software Defined Assets (SDAs). Assets are the main
model of Dagster. You can define assets that are reloaded every day and saved
in S3. Same way as Prefect, you can store the results of assets in many places
in a completely transparent way. Besides, it has many more features than
Prefect.

### Pros

- Best visibility
- Fully managed

### Cons

- More complex than Prefect
- Fully managed (might get expensive)


## Conclusion

| Service/Feature |  Pricing  | Easy (to use) | Visibility | Scheduling | Features | Events | Self hosted | Managed hosting |
|-----------------|:---------:|:-------------:|:----------:|:----------:|:--------:|:------:|:-----------:|:---------------:|
| Luigi           |    ECS    |      ***      |      *     |     NA     |     *    |   NA   |     Yes     |        No       |
| Prefect         |     ?     |       **      |     **     |     Yes    |    **    |   NA   |     Yes     |       Yes*      |
| Dagster         | On demand |       *       |     ***    |     Yes    |    ***   |   Yes  |     Yes     |       Yes       |

[1]: https://github.com/spotify/luigi
