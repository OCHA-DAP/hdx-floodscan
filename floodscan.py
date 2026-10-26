# !/usr/bin/python
"""
HDX Pipeline:
------------

- This pipeline produces two resources for FloodScan:
    - Zonal stats for the most recently available data
    - Geotiffs for the past 90 days

"""
import logging
import os
import re
import shutil
from datetime import datetime
from os.path import basename, isfile, join
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pandas as pd
import ocha_stratus as stratus
import rioxarray as rxr
import xarray as xr
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_sets_add
from slugify import slugify

from src.utils import pg
from src.utils import return_periods as rp
from src.utils.date_utils import (
    create_date_range,
    get_start_and_last_date_from_90_days,
)

logger = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"
# Data plane: which DB + blob storage account to read from. The HDX dataset is
# built from prod data; the env override exists only for testing against dev.
STAGE = os.getenv("STAGE", "prod")
RASTER_CONTAINER = "raster"
POLYGON_CONTAINER = "polygon"


class Floodscan:
    def __init__(self, configuration, retriever, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.manual_url = None
        self.dataset_data = {}
        self.dataset_metadata = {}
        self.start_date = None
        self.latest_date = None
        self.stage = STAGE

    def get_data(self):
        # download zonal stats
        merged_zonal_stats_admin1 = self.get_zonal_stats_for_admin(
            mode=self.stage, admin_level=1, band="SFED"
        )
        merged_zonal_stats_admin2 = self.get_zonal_stats_for_admin(
            mode=self.stage, admin_level=2, band="SFED"
        )
        iso3s = list(merged_zonal_stats_admin1["iso3"]) + list(merged_zonal_stats_admin2["iso3"])
        iso3s = ["global"] + sorted(set(iso3s))

        last90_days_files = self._get_latest_90_days_geotiffs()
        historical_baseline = self._get_historical_baseline()
        last90_days_files = self._generate_geotiffs(
            last90_days_files, historical_baseline
        )
        country_folders = self._clip_last90_days_files(last90_days_files, iso3s)

        # Find the minimum and maximum dates
        (
            self.dataset_metadata["start_date"],
            self.dataset_metadata["end_date"],
        ) = get_start_and_last_date_from_90_days(last90_days_files)

        for iso3 in iso3s:
            country_folder = country_folders[iso3]

            # save all geotiffs as one zipped file
            last90_days_file = shutil.make_archive(
                join(country_folder, self.configuration["90days_filename"]),
                "zip",
                join(country_folder, "geotiffs"),
            )
            excel_file = join(country_folder, self.configuration["stats_filename"])
            shutil.copy(join("files", "floodscan_readme.xlsx"), excel_file)

            with pd.ExcelWriter(
                excel_file,
                mode="a",
                engine="openpyxl",
                if_sheet_exists="replace",
            ) as excel_merged_file:
                if iso3 == "global":
                    zonal_stats_admin1 = merged_zonal_stats_admin1
                    zonal_stats_admin2 = merged_zonal_stats_admin2
                else:
                    zonal_stats_admin1 = merged_zonal_stats_admin1[merged_zonal_stats_admin1["iso3"] == iso3]
                    zonal_stats_admin2 = merged_zonal_stats_admin2[merged_zonal_stats_admin2["iso3"] == iso3]
                zonal_stats_admin1.to_excel(
                    excel_merged_file, sheet_name="admin1", index=False
                )
                zonal_stats_admin2.to_excel(
                    excel_merged_file, sheet_name="admin2", index=False
                )
            self.dataset_data[iso3] = {
                "excel": excel_file,
                "geotiff": last90_days_file,
            }

        return [{"iso3": iso3} for iso3 in iso3s]

    def get_adm_labels(self, df_adm2_90d, level):
        admin_lookup = self.retriever.download_file(
            url="admin_lookup.parquet",
            container=POLYGON_CONTAINER,
            blob="admin_lookup.parquet",
            stage=self.stage,
        )

        df_parquet_labels = pd.read_parquet(admin_lookup)

        # %%
        df_labels = df_parquet_labels[df_parquet_labels.ADM_LEVEL == level]

        df_fs_labelled = pd.merge(
            df_adm2_90d,
            df_labels,
            left_on=["iso3", "pcode"],
            right_on=["ISO3", f"ADM{level}_PCODE"],
            how="left",
        )
        cols_subset = [
            "iso3",
            "ADM0_PCODE",
            "ADM0_NAME",
            "ADM1_PCODE",
            "ADM1_NAME",
            "ADM2_PCODE",
            "ADM2_NAME",
            "valid_date",
            "value",
        ]

        df_fs_labelled_subset = df_fs_labelled[cols_subset]

        countries = []
        for iso3 in df_fs_labelled_subset["iso3"]:
            countries.append(Country.get_country_name_from_iso3(iso3))
            dict_of_sets_add(self.dataset_metadata, "iso3s", iso3)
        df_fs_labelled_subset["ADM0_NAME"] = countries

        return df_fs_labelled_subset

    def get_zonal_stats_for_admin(self, mode, admin_level, band):
        df_current = pg.fs_last_90_days(
            mode=mode, admin_level=admin_level, band=band, only_HRP=True
        )
        df_with_labels = self.get_adm_labels(df_current, admin_level)
        df_current = df_with_labels.rename(
            columns={f"ADM{admin_level}_PCODE": "pcode"}
        )
        df_yr_max = pg.fs_year_max(
            mode=mode, admin_level=admin_level, band=band
        )
        df_w_rps = rp.fs_add_rp(
            df=df_current, df_maxima=df_yr_max, by=["iso3", "pcode"]
        )
        df_w_rps = df_w_rps.rename(columns={"value": band})
        df_w_rps["doy"] = pd.to_datetime(df_w_rps["valid_date"]).dt.dayofyear
        df_rolling_11_day_mean = pg.fs_rolling_11_day_mean(
            mode=mode, admin_level=admin_level, band=band, only_HRP=True
        )

        df_rolling_11_day_mean.iso3 = df_rolling_11_day_mean.iso3.astype(str)
        df_rolling_11_day_mean.pcode = df_rolling_11_day_mean.pcode.astype(str)

        df_w_rps.valid_date = df_w_rps.valid_date.astype(str)
        df_w_rps.iso3 = df_w_rps.iso3.astype(str)
        df_w_rps.pcode = df_w_rps.pcode.astype(str)

        merged_zonal_stats = df_w_rps.merge(
            df_rolling_11_day_mean, on=["iso3", "pcode", "doy"], how="left"
        )
        merged_zonal_stats = merged_zonal_stats.rename(
            columns={"sfed_baseline": "SFED_BASELINE"}
        )
        merged_zonal_stats = merged_zonal_stats.drop("doy", axis=1)

        return merged_zonal_stats

    def generate_dataset(self, iso3):
        dataset_name = self.configuration["dataset_name"]
        title = self.configuration["dataset_title"]
        if iso3 != "global":
            country_name = Country.get_country_name_from_iso3(iso3)
            dataset_name = slugify(f"{dataset_name}-{iso3}")
            title = f"{country_name} - {title}"
        dataset = Dataset({"name": dataset_name, "title": title})
        dataset["notes"] = self.configuration["notes"]
        dataset.add_tags(self.configuration["tags"])

        # time period
        start_date = self.dataset_metadata.get("start_date")
        end_date = self.dataset_metadata.get("end_date")
        if not start_date or not end_date:
            logger.error(f"Date missing for {dataset_name}")
            return None
        dataset.set_time_period(start_date, end_date)

        if iso3 == "global":
            iso3s = self.dataset_metadata["iso3s"]
            dataset.add_country_locations(iso3s)
        else:
            dataset.add_country_location(iso3)

        resource = Resource(
            {
                "name": self.configuration["stats_filename"],
                "description": self.configuration["description_stats_file"],
            }
        )
        resource.set_file_to_upload(self.dataset_data[iso3]["excel"])
        resource.set_format("xlsx")
        dataset.add_update_resource(resource)

        resource = Resource(
            {
                "name": self.configuration["90days_filename"] + ".zip",
                "description": self.configuration["description_90days_file"],
            }
        )
        resource.set_file_to_upload(self.dataset_data[iso3]["geotiff"])
        resource.set_format("zipped geotiff")
        dataset.add_update_resource(resource)

        return dataset

    def subset_band(self, da, band="SFED"):
        long_name = np.array(da.attrs["long_name"])
        index_band = np.where(long_name == band)[0]
        da_subset = da.isel(band=index_band)
        da_subset.attrs["long_name"] = band
        return da_subset

    def raster_container_client(self):
        return stratus.get_container_client(RASTER_CONTAINER, stage=self.stage)

    def polygon_container_client(self):
        return stratus.get_container_client(POLYGON_CONTAINER, stage=self.stage)

    def _get_latest_90_days_geotiffs(self):
        das = {}

        existing_files = [
            x.name
            for x in self.raster_container_client().list_blobs(
                name_starts_with=f"{self.configuration['processed_blob_path']}/aer_area"
            )
        ]

        latest_available_file = sorted(existing_files)[-1]
        search_str = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
        search_res = re.search(search_str, latest_available_file)
        latest_available_date = datetime.strptime(search_res[0], DATE_FORMAT)
        dates = create_date_range(90, latest_available_date)

        for date in dates:
            blob = f"{self.configuration['processed_blob_path']}/aer_area_300s_v{date.strftime(DATE_FORMAT)}_v05r01.tif"

            if blob in existing_files:
                geotiff_file_for_date = self.retriever.download_file(
                    url=blob,
                    container=RASTER_CONTAINER,
                    blob=blob,
                    stage=self.stage,
                )

                da_in = rxr.open_rasterio(geotiff_file_for_date, chunks="auto")
                das[date] = da_in.sel({"band": 1}, drop=True)
            else:
                logger.warning(f"Missing blob {blob} for date {date.strftime(DATE_FORMAT)}.")

        return das

    def _clip_last90_days_files(self, last90_days_files, iso3s):
        country_folders = {}
        clip_bounds = {}

        # download country boundaries and get bounding boxes
        for iso3 in iso3s:
            if iso3 == "global":
                country_folders[iso3] = self.folder
                continue
            country_folder = join(self.folder, iso3)
            country_folders[iso3] = country_folder
            os.makedirs(join(self.folder, iso3, "geotiffs"), exist_ok=True)

            # download shapefiles
            blob_client = self.polygon_container_client().get_blob_client(f"{iso3.lower()}_shp.zip")
            temp_path = join(country_folder, f"{iso3}_shapefile.zip")
            with open(temp_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            with ZipFile(temp_path, "r") as zip_ref:
                zip_ref.extractall(country_folder)
            gdf = gpd.read_file(join(country_folder, f"{iso3.lower()}_adm0.shp"))
            minx, miny, maxx, maxy = gdf.total_bounds
            clip_bounds[iso3] = minx, miny, maxx, maxy

        # load and clip each raster
        for file in last90_days_files:
            ds = rxr.open_rasterio(file)
            for iso3, bounds in clip_bounds.items():
                minx, miny, maxx, maxy = bounds
                ds_clip = ds.rio.clip_box(minx, miny, maxx, maxy)
                out_file = join(self.folder, iso3, "geotiffs", basename(file))
                ds_clip.rio.to_raster(out_file, driver="COG")

        return country_folders

    def _get_historical_baseline(self):
        blob = self.configuration["baseline_filename"]

        if not isfile(blob):
            historical_baseline_file = self.retriever.download_file(
                url=blob,
                container=RASTER_CONTAINER,
                blob=blob,
                stage=self.stage,
            )
        else:
            historical_baseline_file = blob

        chunks = {"lat": 1080, "lon": 1080, "time": 1}
        ds_historical_baseline = xr.open_dataset(
            historical_baseline_file, chunks=chunks
        )
        ds_historical_baseline = ds_historical_baseline.rename_vars(
            {"__xarray_dataarray_variable__": "SFED_BASELINE"}
        )

        return ds_historical_baseline

    def _generate_geotiffs(
        self, last90_days_geotiffs, ds_historical_baseline
    ):
        os.makedirs(join(self.folder, "geotiffs"), exist_ok=True)
        out_files = []

        logger.info("Calculating baseline...")
        for tif_date in last90_days_geotiffs:
            da_current = last90_days_geotiffs[tif_date]
            ds_historical_baseline = ds_historical_baseline.persist()

            dt_temp_str = tif_date.strftime("%Y%m%d")
            doy_temp = int(tif_date.strftime("%j"))
            h_sfed_temp = ds_historical_baseline.sel(
                {"dayofyear": doy_temp}, drop=True
            )
            ds_current_sfed = da_current.to_dataset(name="SFED")

            merged_temp = xr.merge(
                [ds_current_sfed.SFED, h_sfed_temp.SFED_BASELINE],
                combine_attrs="drop",
                compat="no_conflicts",
            )
            merged_temp["SFED"] = merged_temp.SFED.rio.write_nodata(
                np.nan, inplace=True
            )
            merged_temp = merged_temp.rio.set_spatial_dims(
                y_dim="y", x_dim="x"
            )
            merged_temp = merged_temp.rio.write_crs(4326)

            # Save geotiff
            out_file = join(self.folder, "geotiffs", f"{int(dt_temp_str)}_aer_floodscan_sfed.tif")
            merged_temp.rio.to_raster(out_file, driver="COG")
            out_files.append(out_file)

        logger.info(
            f"Finished adding baseline geotiffs to {len(out_files)} files."
        )

        return out_files
