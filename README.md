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

Blob access goes through [ocha-stratus](https://github.com/OCHA-DAP/ocha-stratus).
A local `.env` for the publish half needs:

```shell
DSCI_AZ_BLOB_DEV_SAS=<provided on request>    # intermediates (projects/hdx-floodscan/intermediate/)
DSCI_AZ_BLOB_PROD_SAS=<provided on request>   # rasters + admin lookup
# HDX
HDX_SITE=prod
HDX_KEY=<hdx bot token>
USER_AGENT=<user agent>
PREPREFIX=<preprefix>
```

and for the prepare half (the DB queries), the prod read creds:

```shell
DSCI_AZ_DB_PROD_HOST=<provided on request>
DSCI_AZ_DB_PROD_UID=<provided on request>
DSCI_AZ_DB_PROD_PW=<provided on request>
DSCI_AZ_BLOB_DEV_SAS_WRITE=<provided on request>
```

## How it runs

The rasterstats DB is reachable only through its private endpoint, so the
pipeline is split in two:

1. **Databricks job `HDX FloodScan Prepare`** (`databricks.yml`, 00:15 UTC,
   after `Run FloodScan` in ds-raster-pipelines has landed the day's zonal
   stats). Runs the three `public.floodscan` queries in `src/utils/pg.py`
   (`scripts/prepare_intermediates.py`), writes them as parquet to the dev
   blob under `projects/hdx-floodscan/intermediate/{latest,YYYY-MM-DD}/` with a
   `manifest.json`, then dispatches the GitHub workflow with the
   `GH_FLOODSCAN_TOKEN` dsci secret. No HDX credentials on Databricks.
2. **GitHub workflow `run-python-script.yaml`** — the publisher. Reads the
   intermediates from blob (same `pg.fs_*` functions, now blob-backed), the
   90-day COGs and admin lookup from the prod blob, builds the two resources
   and updates the HDX dataset with the HDX org secrets. Guard:
   `scripts/check_intermediates.py` skips the publish (green) when the manifest
   is older than 6 h — the upstream `Run FloodScan` job still dispatches this
   workflow before the prepare job has run. Dispatch by hand with `force=true`
   to publish whatever is in `latest/`.

Deploying the job (config changes only; code ships by pushing `main`):

```shell
databricks bundle validate -t prod -p DEFAULT
databricks bundle deploy   -t prod -p DEFAULT
```

Local dry run of the prepare half (no dispatch): `python scripts/prepare_intermediates.py --no-dispatch`.

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
