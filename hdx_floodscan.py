#!/usr/bin/python
"""
HDX Pipeline:
------------

TODO
- Add summary about this dataset pipeline

"""
import logging
import os
from copy import copy
from datetime import datetime, timezone
import pandas as pd
from hdx.data.dataset import Dataset
from slugify import slugify
import xarray as xr
import rioxarray as rxr
import numpy as np
import shutil

from src.utils import pg
from src.utils import return_periods as rp

from src.utils.date_utils import create_date_range, get_start_and_last_date_from_90_days

logger = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"


class HDXFloodscan:
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

    def get_data(self):

        try:
            url = os.environ["BLOB_URL"]
            account = os.environ["STORAGE_ACCOUNT"]
            container = os.environ["CONTAINER"]
            key = os.environ["KEY"]
        except Exception:
            url = self.configuration["url"]
            account = self.configuration["account"]
            container = self.configuration["container"]
            key = self.configuration["key"]

        last90_days_filename = self.configuration["last90_days_filename"]
        stats_filename = self.configuration["stats_filename"]
        dataset_name = self.configuration["dataset_names"]["HDX-FLOODSCAN"]

        last90_days_files = self._get_latest_90_days_geotiffs(account, container, key)
        historical_baseline = self._get_historical_baseline(account, container, key)
        last90_days_file = self._generate_zipped_file(last90_days_files, historical_baseline)

        # Find the minimum and maximum dates
        self.start_date, self.latest_date = get_start_and_last_date_from_90_days(last90_days_file)

        # Save all geotiffs as one zipped file
        last90_days_file = shutil.make_archive("baseline_zipped_file", 'zip', "geotiffs")
        shutil.rmtree("geotiffs")

        merged_zonal_stats_admin1 = self.get_zonal_stats_for_admin(mode="prod", admin_level=1, band='SFED')
        merged_zonal_stats_admin2 = self.get_zonal_stats_for_admin(mode="prod", admin_level=2, band='SFED')

        with pd.ExcelWriter('hdx_floodscan_zonal_stats.xlsx') as excel_merged_file:
            pd.DataFrame({'README': ['This is a placeholder']}).to_excel(excel_merged_file, sheet_name='readme')
            merged_zonal_stats_admin1.to_excel(excel_merged_file, sheet_name='admin1')
            merged_zonal_stats_admin2.to_excel(excel_merged_file, sheet_name='admin2')
        self.dataset_data[dataset_name] = [merged_zonal_stats_admin1.apply(lambda x: x.to_dict(), axis=1),
                                           last90_days_file,
                                           excel_merged_file]

        self.created_date = datetime.today().date()
        return [{"name": dataset_name}]

    def get_zonal_stats_for_admin(self, mode, admin_level, band):
        df_current = pg.fs_last_90_days(mode=mode, admin_level=admin_level, band=band)
        df_yr_max = pg.fs_year_max(mode=mode, admin_level=admin_level, band=band)
        df_w_rps = rp.fs_add_rp(
            df=df_current, df_maxima=df_yr_max, by=["iso3", "pcode"]
        )
        df_w_rps = df_w_rps.rename(columns={'value': band})
        df_rolling_11_day_mean = pg.fs_rolling_11_day_mean(
            mode=mode, admin_level=admin_level, band=band
        )
        df_rolling_11_day_mean['valid_date'] = pd.to_datetime(
            datetime.today().year * 1000 + df_rolling_11_day_mean['doy'], format='%Y%j').dt.strftime(DATE_FORMAT)
        df_rolling_11_day_mean = df_rolling_11_day_mean.drop('doy', axis=1)
        df_rolling_11_day_mean.valid_date = df_rolling_11_day_mean.valid_date.astype(str)
        df_rolling_11_day_mean.iso3 = df_rolling_11_day_mean.iso3.astype(str)
        df_rolling_11_day_mean.pcode = df_rolling_11_day_mean.pcode.astype(str)

        df_w_rps.valid_date = df_w_rps.valid_date.astype(str)
        df_w_rps.iso3 = df_w_rps.iso3.astype(str)
        df_w_rps.pcode = df_w_rps.pcode.astype(str)

        merged_zonal_stats = df_w_rps.merge(df_rolling_11_day_mean, on=["iso3", "pcode", "valid_date"], how="left")
        merged_zonal_stats = merged_zonal_stats.rename(columns={'sfed_baseline': "SFED_BASELINE"})

        return merged_zonal_stats

    def generate_dataset_and_showcase(self, dataset_name):

        # Setting metadata and configurations
        name = self.configuration["dataset_names"]["HDX-FLOODSCAN"]
        title = self.configuration["title"]
        update_frequency = self.configuration["update_frequency"]
        dataset = Dataset({"name": slugify(name), "title": title})
        rows = self.dataset_data[dataset_name][0]
        dataset.set_maintainer(self.configuration["maintainer_id"])
        dataset.set_organization(self.configuration["organization_id"])
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        dataset["notes"] = self.configuration["notes"]
        filename = "hdx_floodscan_zonal_stats.xlsx"
        resource_data = {"name": filename,
                         "description": self.configuration["description_stats_file"]}
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
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        for row in rows:
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
            filename,
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )
        res = dataset.get_resource(0)
        res['name'] = "hdx_floodscan_zonal_stats.xlsx"
        res['description'] = self.configuration["description_stats_file"]
        res.set_file_to_upload(self.dataset_data[dataset_name][2])
        res.set_format("xlsx")
        dataset.add_update_resource(res)

        second_filename = "aer_floodscan_300s_SFED_90d.zip"
        resource_data = {"name": second_filename,
                         "description": self.configuration["description_90days_file"]}

        res = copy(dataset.get_resource(0))
        dataset.resources.append(res)
        resource = dataset.get_resource(1)
        resource.set_format("zipped geotiff")
        resource['name'] = resource_data['name']
        resource['description'] = resource_data['description']
        resource.set_file_to_upload(self.dataset_data[dataset_name][1])
        dataset.add_update_resource(resource)

        return dataset

    def _get_latest_90_days_geotiffs(self, account, container, key):

        das = {}
        # TODO add back yesterday = datetime.today() - pd.DateOffset(days=1)
        yesterday = datetime.strptime("2024-01-01", "%Y-%m-%d")
        dates = create_date_range(90, yesterday)

        for date in dates:
            blob = f"floodscan/daily/v5/processed/aer_area_300s_v{date.strftime(DATE_FORMAT)}_v05r01.tif"

            geotiff_file_for_date = self.retriever.download_file(
                url=blob,
                account=account,
                container=container,
                key=key,
                blob=blob)
            da_in = rxr.open_rasterio(geotiff_file_for_date, chunks="auto")
            das[date] = da_in.sel({'band' : 1}, drop=True)

        return das

    def _get_historical_baseline(self, account, container, key):

        blob = f"floodscan/daily/v5/raw/baseline_v2024-01-31_v05r01.nc4"
        historical_baseline_file = self.retriever.download_file(
                url=blob,
                account=account,
                container=container,
                key=key,
                blob=blob)

        chunks = {"lat": 1080, "lon": 1080, "time": 1}
        ds_historical_baseline = xr.open_dataset(historical_baseline_file, chunks=chunks)
        ds_historical_baseline = ds_historical_baseline.rename_vars(
            {"__xarray_dataarray_variable__": "SFED_BASELINE"}
        )

        return ds_historical_baseline

    def _generate_zipped_file(self, last90_days_geotiffs, ds_historical_baseline):

        os.makedirs('geotiffs', exist_ok=True)

        out_files = []
        for tif_date in last90_days_geotiffs:
            da_current = last90_days_geotiffs[tif_date]
            ds_historical_baseline = ds_historical_baseline.persist()

            dt_temp_str = tif_date.strftime("%Y%m%d")
            doy_temp = int(tif_date.strftime('%j'))
            h_sfed_temp = ds_historical_baseline.sel({"dayofyear": doy_temp}, drop=True)

            ds_current_sfed = da_current.to_dataset(name="SFED")

            merged_temp = xr.merge([ds_current_sfed.SFED, h_sfed_temp.SFED_BASELINE], combine_attrs="drop")
            merged_temp['SFED'] = merged_temp.SFED.rio.write_nodata(np.nan, inplace=True)
            merged_temp = merged_temp.rio.set_spatial_dims(y_dim="y", x_dim="x")
            merged_temp = merged_temp.rio.write_crs(4326)

            out_file = f"geotiffs/{int(dt_temp_str)}_aer_floodscan_sfed.tif"

            # Save geotiff
            merged_temp.rio.to_raster(out_file, driver="COG")
            out_files.append(out_file)

        logger.info("Finished adding baseline geotiffs!")
        return out_files



