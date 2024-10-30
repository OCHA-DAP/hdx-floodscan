import os

from dotenv import load_dotenv

load_dotenv()


def cog_url(mode, container_name, cog_name):
    blob_sas = os.getenv(f"DSCI_AZ_SAS_{mode.upper()}")
    return f"https://imb0chd0{mode}.blob.core.windows.net/{container_name}/{cog_name}?{blob_sas}"  # noqa
