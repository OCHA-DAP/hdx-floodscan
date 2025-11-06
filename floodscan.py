# !/usr/bin/python
"""
HDX Pipeline:
------------

- This pipeline produces two datasets for FloodScan:
    - Zonal stats for the most recently available data
    - Geotiffs for the past 90 days

"""
import logging
import os
import os.path
import re
import shutil
from copy import copy
from datetime import datetime

import numpy as np
import pandas as pd
import rioxarray as rxr
import xarray as xr
from azure.storage.blob import BlobServiceClient
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from slugify import slugify

from src.utils import pg
from src.utils import return_periods as rp
from src.utils.date_utils import (
    create_date_range,
    get_start_and_last_date_from_90_days,
)

logger = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"


class Floodscan:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.manual_url = None
        self.dataset_data = {}
        self.errors = errors
        self.created_date = None
        self.start_date = None
        self.latest_date = None

        try:
            self.account = os.environ["STORAGE_ACCOUNT"]
            self.container = os.environ["CONTAINER"]
            self.key = os.environ["KEY"]
        except Exception:
            self.account = self.configuration["account"]
            self.container = self.configuration["container"]
            self.key = self.configuration["key"]

    def get_data(self):

        dataset_name = self.configuration["dataset_names"]["HDX-FLOODSCAN"]

        last90_days_files = self._get_latest_90_days_geotiffs(
            self.account, self.container, self.key
        )
        historical_baseline = self._get_historical_baseline(
            self.account, self.container, self.key
        )
        last90_days_file = self._generate_zipped_file(
            last90_days_files, historical_baseline
        )

        # Find the minimum and maximum dates
        (
            self.start_date,
            self.latest_date,
        ) = get_start_and_last_date_from_90_days(last90_days_file)

        # save all geotiffs as one zipped file
        last90_days_file = shutil.make_archive(
            "baseline_zipped_file", "zip", "geotiffs"
        )
        shutil.rmtree("geotiffs")

        merged_zonal_stats_admin1 = self.get_zonal_stats_for_admin(
            mode="prod", admin_level=1, band="SFED"
        )
        merged_zonal_stats_admin2 = self.get_zonal_stats_for_admin(
            mode="prod", admin_level=2, band="SFED"
        )

        with pd.ExcelWriter(
            "files" + os.sep + "floodscan_readme.xlsx",
            mode="a",
            engine="openpyxl",
            if_sheet_exists="replace",
        ) as excel_merged_file:
            merged_zonal_stats_admin1.to_excel(
                excel_merged_file, sheet_name="admin1", index=False
            )
            merged_zonal_stats_admin2.to_excel(
                excel_merged_file, sheet_name="admin2", index=False
            )

        self.dataset_data[dataset_name] = [
            merged_zonal_stats_admin2.apply(lambda x: x.to_dict(), axis=1),
            last90_days_file,
            excel_merged_file,
        ]

        self.created_date = datetime.today().date()
        return [{"name": dataset_name}]

    def get_adm2_labels(self, df_adm2_90d, level):
        admin_lookup = self.retriever.download_file(
            url="admin_lookup.parquet",
            account=self.account,
            container="polygon",
            key=self.key,
            blob="admin_lookup.parquet",
        )

        df_parquet_labels = pd.read_parquet(admin_lookup)

        # %%
        df_labels_adm2 = df_parquet_labels[df_parquet_labels.ADM_LEVEL == 2]

        df_fs_labelled = pd.merge(
            df_adm2_90d,
            df_labels_adm2,
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

        df_fs_labelled_subset["ADM0_NAME"] = countries

        return df_fs_labelled_subset

    def get_zonal_stats_for_admin(self, mode, admin_level, band):
        df_current = pg.fs_last_90_days(
            mode=mode, admin_level=admin_level, band=band, only_HRP=True
        )
        df_with_labels = self.get_adm2_labels(df_current, admin_level)
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

    def generate_dataset_and_showcase(self, dataset_name):
        # Setting metadata and configurations
        name = self.configuration["dataset_names"]["HDX-FLOODSCAN"]
        title = self.configuration["title"]
        dataset = Dataset({"name": slugify(name), "title": title})
        rows = self.dataset_data[dataset_name][0]
        dataset.set_maintainer(self.configuration["maintainer_id"])
        dataset.set_organization(self.configuration["organization_id"])
        dataset.set_expected_update_frequency(
            self.configuration["update_frequency"]
        )
        dataset.set_subnational(False)
        dataset["notes"] = self.configuration["notes"]

        resource_data = {
            "name": self.configuration["stats_filename"],
            "description": self.configuration["description_stats_file"],
        }

        tags = sorted([t for t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        # Setting time period
        start_date = self.start_date
        ongoing = False
        if not start_date:
            logger.error(f"Start date missing for {dataset_name}")
            return None, None
        dataset.set_time_period(start_date, self.latest_date, ongoing)

        headers = rows[0].keys()
        date_headers = [
            h
            for h in headers
            if "date" in h.lower() and type(rows[0][h]) == int
        ]
        for row in rows:
            dataset.add_other_location(row["iso3"])
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            resource_data["name"],
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding="utf-8",
        )
        res = dataset.get_resource(0)
        res["name"] = self.configuration["stats_filename"]
        res["description"] = self.configuration["description_stats_file"]
        res.set_file_to_upload(self.dataset_data[dataset_name][2])
        res.set_format("xlsx")
        dataset.add_update_resource(res)

        resource_data = {
            "name": self.configuration["90days_filename"],
            "description": self.configuration["description_90days_file"],
        }

        res = copy(dataset.get_resource(0))
        dataset._resources.append(res)
        resource = dataset.get_resource(1)
        resource.set_format("zipped geotiff")
        resource["name"] = resource_data["name"]
        resource["description"] = resource_data["description"]
        resource.set_file_to_upload(self.dataset_data[dataset_name][1])
        dataset.add_update_resource(resource)

        return dataset

    def subset_band(self, da, band="SFED"):
        long_name = np.array(da.attrs["long_name"])
        index_band = np.where(long_name == band)[0]
        da_subset = da.isel(band=index_band)
        da_subset.attrs["long_name"] = band
        return da_subset

    def blob_client(self):
        account_url = f"https://{self.account}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=self.key)

    def _get_latest_90_days_geotiffs(self, account, container, key):
        das = {}

        existing_files = [
            x.name
            for x in self.blob_client()
            .get_container_client(container)
            .list_blobs(
                name_starts_with="floodscan/daily/v5/processed/aer_area"
            )
        ]

        latest_available_file = sorted(existing_files)[-1]
        search_str = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
        search_res = re.search(search_str, latest_available_file)
        latest_available_date = datetime.strptime(search_res[0], "%Y-%m-%d")
        dates = create_date_range(90, latest_available_date)

        for date in dates:
            blob = f"floodscan/daily/v5/processed/aer_area_300s_v{date.strftime(DATE_FORMAT)}_v05r01.tif"

            geotiff_file_for_date = self.retriever.download_file(
                url=blob,
                account=account,
                container=container,
                key=key,
                blob=blob,
            )

            da_in = rxr.open_rasterio(geotiff_file_for_date, chunks="auto")
            das[date] = da_in.sel({"band": 1}, drop=True)

        return das

    def _get_historical_baseline(self, account, container, key):
        blob = self.configuration["baseline_filename"]

        if not os.path.isfile(blob):
            historical_baseline_file = self.retriever.download_file(
                url=blob,
                account=account,
                container=container,
                key=key,
                blob=blob,
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

    def _generate_zipped_file(
        self, last90_days_geotiffs, ds_historical_baseline
    ):
        os.makedirs("geotiffs", exist_ok=True)
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
            )
            merged_temp["SFED"] = merged_temp.SFED.rio.write_nodata(
                np.nan, inplace=True
            )
            merged_temp = merged_temp.rio.set_spatial_dims(
                y_dim="y", x_dim="x"
            )
            merged_temp = merged_temp.rio.write_crs(4326)

            # Save geotiff
            out_file = f"geotiffs/{int(dt_temp_str)}_aer_floodscan_sfed.tif"
            merged_temp.rio.to_raster(out_file, driver="COG")
            out_files.append(out_file)

        logger.info(
            f"Finished adding baseline geotiffs to {len(out_files)} files."
        )

        return out_files
