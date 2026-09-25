"""FloodScan zonal-stats inputs for the HDX publish.

Two halves, split on purpose:

* **DB side** (``query_*``, run on Databricks by ``scripts/prepare_intermediates.py``):
  the SQL against ``public.floodscan`` on the rasterstats DB. The DB is reachable
  only through its private endpoint, so nothing outside the Azure VNet can run
  these. Host/creds come from the ``DSCI_AZ_DB_*`` env vars via ocha-stratus.
* **Blob side** (``fs_*``, run by the GitHub Actions publisher): the same three
  DataFrames read back as parquet from the dev blob, where the prepare job
  parked them. Same function names, arguments and frame shapes as before, so
  ``floodscan.py`` is unchanged.

Blob layout (dev ``projects`` container)::

    hdx-floodscan/intermediate/latest/<name>.parquet + manifest.json
    hdx-floodscan/intermediate/<YYYY-MM-DD>/...            (dated copies)

``<name>`` = ``{kind}_adm{level}_{band}[_hrp]`` — see ``intermediate_blob_name``.
"""

import json
import os

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Where the prepare job parks the query results. Deliberately the DEV blob
# (the team's scratch/projects store) even though the DB read is prod: these
# are intermediates, not a product.
INTERMEDIATE_STAGE = "dev"
INTERMEDIATE_CONTAINER = "projects"
INTERMEDIATE_PREFIX = "hdx-floodscan/intermediate"
MANIFEST_NAME = "manifest.json"


def get_engine(mode):
    """Read-only engine for the rasterstats DB via ocha-stratus (DB side)."""
    return stratus.get_engine(stage=mode)


# --------------------------------------------------------------------------- #
# DB side
# --------------------------------------------------------------------------- #


def query_year_max(engine, admin_level, band="SFED"):
    query_yr_max = f"""
        SELECT iso3, pcode, DATE_TRUNC('year', valid_date) AS year_date, MAX(mean) AS value
        FROM floodscan
        WHERE adm_level = {admin_level}
          AND band = '{band}'
          AND valid_date <= '2023-12-31'
        GROUP BY iso3, pcode, year_date
    """
    return pd.read_sql(sql=query_yr_max, con=engine)


def query_rolling_11_day_mean(engine, admin_level, band="SFED", only_HRP=False):
    only_HRP_clause = (
        f" AND iso3 IN (SELECT iso3 FROM iso3 WHERE has_active_hrp=true)"
        if only_HRP
        else None
    )

    query_rolling_mean = f"""
        WITH filtered_data AS (
            SELECT iso3, pcode, valid_date, mean
            FROM floodscan
            WHERE adm_level = {admin_level}
                AND band = '{band}'
                AND valid_date >= DATE_TRUNC('year', NOW()) - INTERVAL '10 years'
                AND valid_date < DATE_TRUNC('year', NOW()) {only_HRP_clause}
        ),
        rolling_mean AS (
            SELECT iso3, pcode, valid_date,
                    AVG(mean) OVER (PARTITION BY iso3, pcode ORDER BY valid_date
                                    ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS rolling_mean
            FROM filtered_data
        ),
        doy_mean AS (
            SELECT iso3, pcode, EXTRACT(DOY FROM valid_date) AS doy,
                    AVG(rolling_mean) AS SFED_BASELINE
            FROM rolling_mean
            GROUP BY iso3, pcode, doy
        )
        SELECT * FROM doy_mean
    """  # noqa: E202 E231

    return pd.read_sql(sql=query_rolling_mean, con=engine)


def query_last_90_days(engine, admin_level, band="SFED", only_HRP=False):
    query_last_90_days = f"""
    SELECT iso3, pcode, valid_date, mean AS value
    FROM floodscan
    WHERE adm_level = {admin_level}
      AND band = '{band}'
      AND valid_date >= NOW() - INTERVAL '90 days'
    """
    if only_HRP:
        query_last_90_days += (
            f" AND iso3 IN (SELECT iso3 FROM iso3 WHERE has_active_hrp=true)"
        )
    return pd.read_sql(sql=query_last_90_days, con=engine)


# --------------------------------------------------------------------------- #
# Blob side
# --------------------------------------------------------------------------- #


def intermediate_blob_name(kind, admin_level, band, only_HRP=False, folder="latest"):
    suffix = "_hrp" if only_HRP else ""
    return f"{INTERMEDIATE_PREFIX}/{folder}/{kind}_adm{admin_level}_{band}{suffix}.parquet"


def manifest_blob_name(folder="latest"):
    return f"{INTERMEDIATE_PREFIX}/{folder}/{MANIFEST_NAME}"


def read_manifest(folder="latest"):
    """The prepare job's manifest, or None when no intermediates exist yet."""
    try:
        raw = stratus.load_blob_data(
            manifest_blob_name(folder),
            stage=INTERMEDIATE_STAGE,
            container_name=INTERMEDIATE_CONTAINER,
        )
    except Exception as exc:  # noqa: BLE001 — SDK raises ResourceNotFoundError
        if "ResourceNotFound" in type(exc).__name__ or "BlobNotFound" in str(exc):
            return None
        raise
    return json.loads(raw)


def _load(kind, mode, admin_level, band, only_HRP=False):
    manifest = read_manifest()
    if manifest is None:
        raise FileNotFoundError(
            f"no FloodScan intermediates under {INTERMEDIATE_PREFIX}/latest/ — "
            "has the Databricks job 'HDX FloodScan Prepare' run?"
        )
    if manifest.get("db_stage") != mode:
        raise ValueError(
            f"intermediates were prepared from the {manifest.get('db_stage')!r} DB, "
            f"but mode={mode!r} was requested"
        )
    return stratus.load_parquet_from_blob(
        intermediate_blob_name(kind, admin_level, band, only_HRP),
        stage=INTERMEDIATE_STAGE,
        container_name=INTERMEDIATE_CONTAINER,
    )


def fs_year_max(mode, admin_level, band="SFED"):
    return _load("year_max", mode, admin_level, band)


def fs_rolling_11_day_mean(mode, admin_level, band="SFED", only_HRP=False):
    return _load("rolling_11_day_mean", mode, admin_level, band, only_HRP)


def fs_last_90_days(mode, admin_level, band="SFED", only_HRP=False):
    return _load("last_90_days", mode, admin_level, band, only_HRP)
