# HDX FloodScan

## Directory structure

The code in this repository is organized as follows:

```shell
TBD

```

## Development

This repo is being developed with python version 3.12.4

### Setup Instructions

Create a virtual env & activate it. Then install the the requirements with

```shells
pip install -r requirements.txt
```

Next install module code in src using the command:

```shell
pip install -e .
```

### Environment keys

DB and blob access go through [ocha-stratus](https://github.com/OCHA-DAP/ocha-stratus),
so a local `.env` needs the standard team variables (read-only is enough):

```shell
DSCI_AZ_DB_PROD_HOST=<provided on request>
DSCI_AZ_DB_PROD_UID=<provided on request>
DSCI_AZ_DB_PROD_PW=<provided on request>
DSCI_AZ_BLOB_PROD_SAS=<provided on request>
# HDX
HDX_SITE=prod
HDX_KEY=<hdx bot token>
USER_AGENT=<user agent>
PREPREFIX=<preprefix>
```

`STAGE=dev` switches both the DB and the blob account to dev (then the `_DEV_`
variants of the variables above are needed).

## Deployment

The publish runs as the Databricks job **HDX FloodScan Publish**, defined in
`databricks.yml` and deployed with the Databricks CLI:

```shell
databricks bundle validate -t prod -p default
databricks bundle deploy   -t prod -p default
```

It runs on the shared Job Compute policy, which injects the DB/blob secrets;
the HDX credentials come from the `dsci` secret scope (`HDX_KEY`,
`HDX_USER_AGENT`, `HDX_PREPREFIX`). The upstream data are produced by the
`Run FloodScan` job in ds-raster-pipelines (23:00 UTC), which still dispatches
the (now no-op) GitHub workflow in this repo; this job is scheduled 00:15 UTC.

### Formatting

All code is formatted according to black and flake8 guidelines.
The repo is set-up to use pre-commit.
Before you start developing in this repository, you will need to run

```shell
pre-commit install
```

The `markdownlint` hook will require
[Ruby](https://www.ruby-lang.org/en/documentation/installation/)
to be installed on your computer.

You can run all hooks against all your files using

```shell
pre-commit run --all-files
```

It is also **strongly** recommended to use `jupytext`
to convert all Jupyter notebooks (`.ipynb`) to Markdown files (`.md`)
before committing them into version control. This will make for
cleaner diffs (and thus easier code reviews) and will ensure that cell outputs aren't
committed to the repo (which might be problematic if working with sensitive data).
