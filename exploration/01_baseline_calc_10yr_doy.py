"""
This script calculates the DOY baseline for FloodScan raster data at pixel
level. It is currently in `exploratory` repo as it just uses GDRIVE `.nc` file
to make local development more effecient (vs loading historical data from blob)

In the official pipeline we can link it to the historical tif record on the
blob.

Steps:

1. Smooth historical data w/ 10 day rolling mean (centered)
2. filter data to last 10 years of data (exlcusive of current year)
3. Calculate the mean SFED value per day of year from smoothed raster stack.
"""

import datetime

import matplotlib.pyplot as plt

from src.datasources import floodscan

# Get today's date and save as run_date
run_date = datetime.datetime.today()

# Extract the year from run_date
current_year = run_date.year

# Create a sequence of the last 10 years, exclusive of the current year
last_10_years = list(range(current_year - 10, current_year))

# open historical netcdf file from gdrive just bc so much faster than loading
# from the blob
da_fs = floodscan.open_historical_floodscan()

# rolling window to smooth out historical data
da_fs_smooth = da_fs.rolling(time=10, center=True).mean()

# Filter da_fs_smooth to only include dates in last_10_years
da_fs_smooth_filt = da_fs_smooth.sel(
    time=da_fs_smooth["time.year"].isin(last_10_years)
)

# calculate mean per DOY
da_doy_mean = da_fs_smooth_filt.groupby("time.dayofyear").mean("time")


# try to plot -- this is where it really takes some time.
days_to_plot = [1, 10, 200]

fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 5))

for ax, day in zip(axes, days_to_plot):
    da_doy_mean.sel(dayofyear=day).plot(ax=ax)
    ax.set_title(f"Day {day}")

plt.tight_layout()
plt.show()
