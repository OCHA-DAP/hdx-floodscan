# HDX FloodScan

## Directory structure

The code in this repository is organized as follows:

```shell
TBD

```

## Development

### Setup Instructions

Create a virtual env & activate it. Then install the the requirements with

```shells
pip install -r requirements.txt
```

Next install module code in src using the command:

```shell
pip install -e.
```

### Environment keys

```shell
DSCI_AZ_SAS_DEV=<provided on request>
DSCI_AZ_SAS_PROD=<provided on request>
AZURE_DB_PW=<provided on request>
AZURE_DB_UID=<provided on request>
```

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
