import os
from pathlib import Path

import rioxarray as rxr
import xarray as xr

from src.utils import cloud_utils, cog_utils, gen_utils

# from src.utils import blob

DATA_DIR_GDRIVE = Path(os.getenv("AA_DATA_DIR_NEW"))
FP_FS_HISTORICAL = (
    DATA_DIR_GDRIVE
    / "private"
    / "raw"
    / "glb"
    / "FloodScan"
    / "SFED"
    / "SFED_historical"
    / "aer_sfed_area_300s_19980112_20231231_v05r01.nc"
)


def open_historical_floodscan():
    chunks = {"lat": 1080, "lon": 1080, "time": 1}
    ds = xr.open_dataset(FP_FS_HISTORICAL, chunks=chunks)
    da = ds["SFED_AREA"]
    da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    da = da.rio.write_crs(4326)
    return da


def load_floodscan_cogs(
    start_date,
    end_date,
    mode="dev",
    container_name="global",
    prefix="raster/cogs/aer_area_300s",
):
    container_client = cloud_utils.get_container_client(
        mode=mode, container_name=container_name
    )
    cogs_list = [
        x.name
        for x in container_client.list_blobs(name_starts_with=prefix)
        if (gen_utils.extract_date(x.name).date() >= start_date)
        & (gen_utils.extract_date(x.name).date() <= end_date)  # noqa
    ]

    das = []
    for cog in cogs_list:
        da_in = process_floodscan_cog(
            cog_name=cog, container_name="global", mode="dev"
        )
        das.append(da_in)
    return xr.combine_by_coords(das, combine_attrs="drop")


def process_floodscan_cog(cog_name, mode, container_name):
    url_str_tmp = cog_utils.cog_url(
        cog_name=cog_name, mode=mode, container_name=container_name
    )
    da_in = rxr.open_rasterio(url_str_tmp, chunks="auto")
    da_in["date"] = gen_utils.extract_date(url_str_tmp)
    da_in = da_in.expand_dims(["date"])
    da_in = da_in.persist()
    return da_in


def historical_doy_baseline(
    da, current_year, n_baseline_years=10, n_days_smooth=10
):
    last_n_years = list(range(current_year - n_baseline_years, current_year))
    da_smooth = da.rolling(time=n_days_smooth, center=True).mean()
    da_smooth_filt = da_smooth.sel(
        time=da_smooth["time.year"].isin(last_n_years)
    )
    da_doy_mean = da_smooth_filt.groupby("time.dayofyear").mean("time")
    ds_doy_mean = da_doy_mean.to_dataset().rename_vars(
        {"SFED_AREA": "SFED_BASELINE"}
    )
    return ds_doy_mean