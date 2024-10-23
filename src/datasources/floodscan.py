import os
from pathlib import Path
from typing import Literal
import rasterio
import xarray as xr
import rioxarray as rxr

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


# def get_blob_name(
#     iso3: str,
#     data_type: Literal["exposure_raster", "exposure_tabular"],
#     date: str = None,
# ):
#     if data_type == "exposure_raster":
#         if date is None:
#             raise ValueError("date must be provided for exposure data")
#         return (
#             f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
#             f"{iso3}/{iso3}_exposure_{date}.tif"
#         )
#     elif data_type == "exposure_tabular":
#         return (
#             f"{blob.PROJECT_PREFIX}/processed/flood_exposure/tabular/"
#             f"{iso3}_adm_flood_exposure.parquet"
#         )
#     elif data_type == "flood_extent":
#         return (
#             f"{blob.PROJECT_PREFIX}/processed/flood_extent/"
#             f"{iso3}_flood_extent.tif"
#         )
