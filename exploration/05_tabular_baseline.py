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
# ---

# %%
# %matplotlib inline
# %load_ext autoreload
# %autoreload 2

from src.utils import pg

# %% [markdown]
# The function below calculates the following DB side:
# 1. centered 11 day rolling mean of SFED (+/5 days) for the last 10 years of
# data (not inclusive of current year)
# 2. Then calculates the average of that value per Day-of-Year (DOY) over the
# 10 year period.
#
# - The resulting column is called `sfed_baseline`.
#
# - This value will be added to the final tabular data set and should
# match/give the same exact value as an analyst would get if they did took the
# zonal mean of the SFED_BASELINE band in the raster data set (This is a good
# test for us to do)
#
#
# For the sake of efficiency i do this at the admin 1 level in the example
# below. However, I suspect that for the formal pipeline it would be good to do
# this at both admin levels and store the results as either `tables` in
# postgres database or files (parquet?) on blob. They will only need to be
# updated 1x per year so we don't need to have this calculation always running.

# %%
df_doy_means = pg.fs_rolling_11_day_mean(
    mode="prod", admin_level=1, band="SFED"
)

# %%
df_doy_means
