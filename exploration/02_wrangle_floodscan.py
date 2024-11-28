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
#     name: python3
# ---

# %% [markdown]
# This script illustrates the process to produce and merge the HDX FloodScan
# COGS It serves as a proof of concept that can be refined further as
# a pipeline.
#
# The code calculates the SFED Day of Year (DOY) baseline and then merges it
# with the relevant SFED raster and writes out one COG per day with 2 bands:
# SFED & SFED_BASELINE
#
# Note on data inputs:
# 1. Historical data read from `.nc` file on Google Drive. This is used over
# the blob for more effecient loading/development.
# 2. Daily SFED (recent): loaded from dev blob container produced by R-pipeline
# in `ds-floodscan-ingest` repository. When our formal internal FloodScan
# pipeline is complete we can change the blob directory.


# %%
import datetime

import pandas as pd
import xarray as xr

from src.datasources import floodscan
from src.utils import date_utils

# %%
# just a quick utility function to get recent floodscan date that will
# will be in blob. Only doing a couple days just as proof of concept.
end_date = date_utils.date_to_run()
start_date = end_date - datetime.timedelta(days=3)
da_current = floodscan.load_floodscan_cogs(
    start_date=start_date, end_date=end_date
)

ds_current = da_current.to_dataset(dim="band")

bname = list(ds_current.data_vars)[0]
ds_current_sfed = (
    ds_current.rename_vars({bname: "SFED"})
    .rename({"x": "lon", "y": "lat"})
    .rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    .rio.write_crs(4326)
)

# %%
# load data from gdrive
ds_historical = floodscan.open_historical_floodscan()

# %%
# calculate smoothed DOY baseline
ds_historical_baseline = floodscan.historical_doy_baseline(
    da=ds_historical,
    current_year=end_date.year,
    n_baseline_years=10,
    n_days_smooth=11,
)


# %%
# merge historical and current data based on date/doy.
for date_temp in ds_current_sfed["date"].values:
    c_sfed_temp = ds_current_sfed.sel(date=date_temp)
    dt_temp = pd.to_datetime(date_temp)
    doy_temp = dt_temp.dayofyear
    h_sfed_temp = ds_historical_baseline.sel(dayofyear=doy_temp)

    # floating point issue causes lat/lon values from nc to be recognized as
    # different than the COGS in blob even though they are the 'same'
    h_sfed_temp_interp = h_sfed_temp.interp_like(c_sfed_temp, method="nearest")
    merged_temp = xr.merge([c_sfed_temp, h_sfed_temp_interp])
    merged_temp = merged_temp.rio.set_spatial_dims(y_dim="lat", x_dim="lon")
    merged_temp = merged_temp.rio.write_crs(4326)

    dt_temp_str = dt_temp.strftime("%Y%m%d")

    # in actual pipeline will probably save to a tempfile and upload to
    # blob
    out_file = f"{dt_temp_str}_aer_floodscan_sfed_10km.tif"

    # should play w/ compressions and various other gdal configs
    merged_temp.rio.to_raster(out_file, driver="COG")
