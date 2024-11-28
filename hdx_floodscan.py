#!/usr/bin/python
"""
HDX Pipeline:
------------

TODO
- Add summary about this dataset pipeline

"""
import logging
import os
import sys
from pathlib import Path
from copy import copy
from datetime import datetime, timezone
import pandas as pd
from hdx.data.dataset import Dataset
from slugify import slugify
import xarray as xr
import shutil

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
        historical_baseline = self._get_historical_baseline(account, container, key, last90_days_filename)
        last90_days_file = self._generate_zipped_file(last90_days_files, historical_baseline)

        # Find the minimum and maximum dates
        self.start_date, self.latest_date = get_start_and_last_date_from_90_days(last90_days_file)

        # Save all geotiffs as one zipped file
        last90_days_file = shutil.make_archive("baseline_zipped_file", 'zip', "geotiffs")
        shutil.rmtree("geotiffs")

        stats_file = self.retriever.download_file(
            url=url,
            account=account,
            container="dsci", # TODO change this to parameter before pushing changes to prod
            key=key,
            blob=stats_filename)

        data_df_stats = pd.read_excel(stats_file, sheet_name="FloodScan", keep_default_na=False).replace('[“”]', '', regex=True)

        self.dataset_data[dataset_name] = [data_df_stats.apply(lambda x: x.to_dict(), axis=1),
                                           last90_days_file]

        self.created_date = datetime.fromtimestamp((os.path.getctime(stats_file)), tz=timezone.utc)
        return [{"name": dataset_name}]

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

        geotiffs = []
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
            ds = xr.open_dataset(geotiff_file_for_date)
            ds['date'] = date
            geotiffs.append(ds.sel({"band": 1}, drop=True))

        return geotiffs

    def _get_historical_baseline(self, account, container, key, blob):

        historical_baseline_file = self.retriever.download_file(
                url=blob,
                account=account,
                container=container,
                key=key,
                blob=blob)
        ds_historical_baseline = xr.open_dataset(historical_baseline_file)

        return ds_historical_baseline

    def _generate_zipped_file(self, last90_days_geotiffs, ds_historical_baseline):

        base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        geotiffs_dir = Path("geotiffs")
        new_dir = base_dir / geotiffs_dir
        new_dir.mkdir(exist_ok=True)

        out_files = []
        for ds_current_sfed in last90_days_geotiffs:
            ds_historical_baseline = ds_historical_baseline.persist()

            dt_temp_str = ds_current_sfed.date.dt.strftime("%Y%m%d")
            doy_temp = int(ds_current_sfed["date"].dt.dayofyear)
            ds_current_sfed = ds_current_sfed.drop_vars(["date"])
            h_sfed_temp = ds_historical_baseline.sel({"dayofyear": doy_temp}, drop=True)

            h_sfed_temp_interp = h_sfed_temp.interp_like(ds_current_sfed, method="nearest")
            merged_temp = xr.merge([ds_current_sfed, h_sfed_temp_interp])
            merged_temp= merged_temp.drop_vars(["band_data"])
            merged_temp = merged_temp.rename({'__xarray_dataarray_variable__' : "SFED_BASELINE"})

            out_file = f"geotiffs/{int(dt_temp_str)}_aer_floodscan_sfed.tif"

            # Save geotiff
            merged_temp.rio.to_raster(out_file, driver="COG")
            out_files.append(out_file)

        return out_files


