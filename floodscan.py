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
from io import BytesIO
from os.path import basename, isfile, join
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import ocha_stratus as stratus
import pandas as pd
import rioxarray as rxr
import xarray as xr
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_sets_add
from slugify import slugify

from src.utils.date_utils import (
    create_date_range,
    get_start_and_last_date_from_90_days,
)
from src.utils.return_periods import fs_add_rp

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d"


class Floodscan:
    def __init__(self, configuration, save, use_saved, tempdir, savedir):
        self.configuration = configuration
        self.save = save
        self.use_saved = use_saved
        self.folder = savedir if save or use_saved else tempdir
        self.dataset_data = {}
        self.dataset_metadata = {}
        self.start_date = None
        self.latest_date = None
        self.stage = os.environ["BLOB_STAGE"]

    def get_data(self):
        # download zonal stats
        merged_zonal_stats_admin1 = self.get_zonal_stats_for_admin(
            admin_level=1, band="SFED"
        )
        merged_zonal_stats_admin2 = self.get_zonal_stats_for_admin(
            admin_level=2, band="SFED"
        )
        iso3s = list(merged_zonal_stats_admin1["iso3"]) + list(merged_zonal_stats_admin2["iso3"])
        iso3s = ["global"] + list(set(iso3s))

        last90_days_files = self._get_latest_90_days_geotiffs()
        historical_baseline = self._get_historical_baseline()
        last90_days_files = self._generate_geotiffs(
            last90_days_files, historical_baseline
        )
        country_folders = self.clip_last90_days_files(last90_days_files, iso3s)

        # Find the minimum and maximum dates
        (
            self.dataset_metadata["start_date"],
            self.dataset_metadata["end_date"],
        ) = get_start_and_last_date_from_90_days(last90_days_files)

        for iso3 in iso3s:
            country_folder = country_folders[iso3]

            # save all geotiffs as one zipped file
            last90_days_file = shutil.make_archive(
                join(country_folder, self.configuration["90days_filename"]), "zip", join(country_folder, "geotiffs")
            )

            excel_file = join(country_folder, self.configuration["stats_filename"])
            shutil.copy(join("config", "floodscan_readme.xlsx"), excel_file)
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

    def get_adm_labels(self, df_90d, level):
        admin_lookup = stratus.load_parquet_from_blob(
            "admin_lookup.parquet",
            self.stage,
            "polygon",
        )
        df_labels = admin_lookup[admin_lookup.ADM_LEVEL == level]
        df_fs_labelled = pd.merge(
            df_90d,
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
            dict_of_sets_add(self.dataset_metadata, "iso3s", iso3)  # TODO: get country list by dataset
        df_fs_labelled_subset["ADM0_NAME"] = countries

        return df_fs_labelled_subset

    def get_zonal_stats_for_admin(self, admin_level, band):
        engine = stratus.get_engine(self.stage)

        # get list of HRP countries
        iso_query = "SELECT iso3 FROM iso3 WHERE has_active_hrp=true"
        with engine.connect() as con:
            df_iso3 = pd.read_sql_query(iso_query, con)
        iso3_list = df_iso3["iso3"].tolist()
        iso3_list = "('" + "','".join(iso3_list) + "')"

        # use country list to get raster stats from last 90 days
        query = f"""
            SELECT iso3, pcode, valid_date, mean AS value
            FROM floodscan
            WHERE adm_level = {admin_level}
              AND band = '{band}'
              AND valid_date >= NOW() - INTERVAL '90 days'
              AND iso3 IN {iso3_list}
            """
        with engine.connect() as con:
            df_current = pd.read_sql_query(query, con)

        df_with_labels = self.get_adm_labels(df_current, admin_level)
        df_current = df_with_labels.rename(
            columns={f"ADM{admin_level}_PCODE": "pcode"}
        )

        query_yr_max = f"""
            SELECT iso3, pcode, DATE_TRUNC('year', valid_date) AS year_date, MAX(mean) AS value
            FROM floodscan
            WHERE adm_level = {admin_level}
              AND band = '{band}'
              AND valid_date <= '2023-12-31'
            GROUP BY iso3, pcode, year_date
        """
        with engine.connect() as con:
            df_yr_max = pd.read_sql_query(query_yr_max, con)

        df_w_rps = fs_add_rp(
            df=df_current, df_maxima=df_yr_max, by=["iso3", "pcode"]
        )
        df_w_rps = df_w_rps.rename(columns={"value": band})
        df_w_rps["doy"] = pd.to_datetime(df_w_rps["valid_date"]).dt.dayofyear

        query_rolling_mean = f"""
            WITH filtered_data AS (
                SELECT iso3, pcode, valid_date, mean
                FROM floodscan
                WHERE adm_level = {admin_level}
                    AND band = '{band}'
                    AND valid_date >= DATE_TRUNC('year', NOW()) - INTERVAL '10 years'
                    AND valid_date < DATE_TRUNC('year', NOW())
                    AND iso3 IN {iso3_list}
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
        """
        with engine.connect() as con:
            df_rolling_11_day_mean = pd.read_sql_query(query_rolling_mean, con)

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
            return None, None
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

    def _get_latest_90_days_geotiffs(self):
        das = {}

        existing_files = stratus.list_container_blobs(
            name_starts_with=f"{self.configuration['processed_blob_path']}/aer_area",
            stage=self.stage,
            container_name="raster",
        )

        latest_available_file = sorted(existing_files)[-1]
        search_str = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
        search_res = re.search(search_str, latest_available_file)
        latest_available_date = datetime.strptime(search_res[0], DATE_FORMAT)
        dates = create_date_range(90, latest_available_date)

        for date in dates:
            blob = f"{self.configuration['processed_blob_path']}/aer_area_300s_v{date.strftime(DATE_FORMAT)}_v05r01.tif"

            if blob in existing_files:
                geotiff_file_for_date = stratus.open_blob_cog(
                    blob,
                    self.stage,
                    container_name="raster",
                )
                das[date] = geotiff_file_for_date.sel({"band": 1}, drop=True)
            else:
                logger.warning(f"Missing blob {blob} for date {date.strftime(DATE_FORMAT)}.")

        return das

    def clip_last90_days_files(self, last90_days_files, iso3s):
        country_folders = {}
        clip_bounds = {}

        # download country boundaries and get bounding boxes
        for iso3 in iso3s:
            if iso3 == "global":
                country_folders[iso3] = "files"
                continue
            country_folders[iso3] = join("files", iso3)
            os.makedirs(join("files", iso3, "geotiffs"), exist_ok=True)

            # download shapefiles
            container = stratus.get_container_client("polygon", self.stage)
            blob_client = container.get_blob_client(f"{iso3.lower()}_shp.zip")
            temp_path = join(self.folder, f"{iso3}_shapefile.zip")
            with open(temp_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            with ZipFile(temp_path, "r") as zip_ref:
                zip_ref.extractall(join(self.folder, iso3))
            gdf = gpd.read_file(join(self.folder, iso3, f"{iso3.lower()}_adm0.shp"))
            minx, miny, maxx, maxy = gdf.total_bounds
            clip_bounds[iso3] = minx, miny, maxx, maxy

        # load and clip each raster
        for file in last90_days_files:
            ds = rxr.open_rasterio(file)
            for iso3, bounds in clip_bounds.items():
                minx, miny, maxx, maxy = bounds
                ds_clip = ds.sel(x=slice(minx, maxx), y=slice(maxy, miny))
                out_file = join("files", iso3, "geotiffs", basename(file))
                ds_clip.rio.to_raster(out_file, driver="COG")

        return country_folders

    def _get_historical_baseline(self):
        blob = self.configuration[f"baseline_filename_{self.stage}"]
        chunks = {"lat": 1080, "lon": 1080, "time": 1}

        if not isfile(blob):
            historical_baseline = stratus.load_blob_data(
                blob,
                self.stage,
                "raster",
            )
            ds_historical_baseline = xr.open_dataset(
                BytesIO(historical_baseline),
                engine="h5netcdf",
                chunks=chunks,
            )
        else:
            ds_historical_baseline = xr.open_dataset(
                blob, chunks=chunks
            )
        ds_historical_baseline = ds_historical_baseline.rename_vars(
            {"__xarray_dataarray_variable__": "SFED_BASELINE"}
        )

        return ds_historical_baseline

    def _generate_geotiffs(
        self, last90_days_geotiffs, ds_historical_baseline
    ):
        os.makedirs(join("files", "geotiffs"), exist_ok=True)
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
            out_file = join("files", "geotiffs", f"{int(dt_temp_str)}_aer_floodscan_sfed.tif")
            merged_temp.rio.to_raster(out_file, driver="COG")
            out_files.append(out_file)

        logger.info(
            f"Finished adding baseline geotiffs to {len(out_files)} files."
        )

        return out_files
