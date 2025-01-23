# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: hdx-floodscan
#     language: python
#     name: hdx-floodscan
# ---

# %%
# %matplotlib inline
# %load_ext autoreload
# %autoreload 2


# %% [markdown]
# - Quick script to make sure our calculations to calcualte baseline values
# from both the raster (xarray) and tabular (SQL) are harmonized/ in sync.
# - to do this we ran zonal stats on the raster baseline bands on the COG files
# in stage -- using the dsci zonal stats upsampling methodology.
# - The resulting tables  have been saved in our dev database.
# - In this script we then run the tabular baseline SQL method on the SFED
#   data in prod.
# - Note the `pg.fs_rolling_11_day_mean()` has been modified slightly in this
# script to allow user to put in different years to initiated the end date of
# the baseline calculation. This is done as stage files have been developed
# and will continue to be developed with different ranges for baseline
# calculations.
#

# %%

import pandas as pd

from src.utils import pg


# %%
# modify for custom run_year
def fs_rolling_baseline_modified(
    mode, admin_level, band="SFED", run_year=None
):
    engine = pg.get_engine(mode)
    if run_year is None:
        start_date = "DATE_TRUNC('year', NOW()) - INTERVAL '10 years'"
        end_date = "DATE_TRUNC('year', NOW())"
    else:
        # i wrote in this syntax to specifically make sure the
        # DATE_TRUNC, NOW(), and INTERVAL functions were working correctly.
        sd = f"'{run_year}-01-01'::timestamp"  # noqa: E202 E231
        ed = f"'{run_year}-12-31'::timestamp"  # noqa: E202 E231
        start_date = f"DATE_TRUNC('year', {sd}) - INTERVAL '10 years'"
        end_date = f"DATE_TRUNC('year', {ed})"

    query_rolling_mean = f"""
        WITH filtered_data AS (
            SELECT iso3, pcode, valid_date, mean
            FROM floodscan
            WHERE adm_level = {admin_level}
                AND band = '{band}'
                AND valid_date >= {start_date}
                AND valid_date < {end_date}
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


# %% [markdown]
# get `baseline` data from dev database and add `doy` column

# %%

dev_engine = pg.get_engine(mode="dev")

dev_query = """
    SELECT *
    FROM baseline
    WHERE adm_level = 1
"""

df_pg_baseline = pd.read_sql(sql=dev_query, con=dev_engine)
df_pg_baseline["valid_date"] = pd.to_datetime(df_pg_baseline["valid_date"])
df_pg_baseline["doy"] = df_pg_baseline["valid_date"].dt.dayofyear


# %% [markdown]
# use `fs_folling_baseline_modified()` to calculate the doy baselines using
# 2014-2023 data as done for the raster files used as input into the zonal
# statistics tables in baseline on dev

# %%
df_doy_means = fs_rolling_baseline_modified(
    mode="prod", admin_level=1, band="SFED", run_year=2024
)

# %% [markdown]
# Merge files and check for differences

# %%
df_merged = pd.merge(
    df_pg_baseline, df_doy_means, on=["iso3", "pcode", "doy"], how="inner"
)

df_merged.rename(
    columns={"mean": "baseline_zonal", "sfed_baseline": "baseline_tabular"},
    inplace=True,
)


# %%

df_selected = df_merged[
    [
        "iso3",
        "pcode",
        "valid_date",
        "doy",
        "baseline_zonal",
        "baseline_tabular",
    ]
]
df_selected

# %% [markdown]
# All good - there are some slight differences likely due to floating point
# issues. Therefore we set a very low difference tolerance of 1e-7

# %%
# Check for floating point differences
tolerance = 1e-7
df_differences = df_selected[
    (df_selected["baseline_zonal"] - df_selected["baseline_tabular"]).abs()
    > tolerance
]

# Display the rows with differences
df_differences
