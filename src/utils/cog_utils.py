# import logging
# from typing import Literal
# import coloredlogs
import os

# import pandas as pd
# import rioxarray as rxr
# import tqdm
# import xarray as xr

# from src.config.settings import load_pipeline_config
# from src.utils.cloud_utils import get_cog_url, get_container_client

# logger = logging.getLogger(__name__)
# coloredlogs.install(level="DEBUG", logger=logger)


def cog_url(mode, container_name, cog_name):
    blob_sas = os.getenv(f"DSCI_AZ_SAS_{mode.upper()}")
    return f"https://imb0chd0{mode}.blob.core.windows.net/{container_name}/{cog_name}?{blob_sas}"  # noqa
