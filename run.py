#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os import makedirs
from os.path import basename, dirname, exists, expanduser, join
from tempfile import gettempdir
from typing import Any, Callable, Optional  # noqa: F401

import ocha_stratus as stratus

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from floodscan import Floodscan


logger = logging.getLogger(__name__)

_LOOKUP = "hdx-floodscan"
_SAVED_DATA_DIR = "saved_data"
_UPDATED_BY_SCRIPT = "HDX Scraper: FloodScan"


class AzureBlobDownload(Download):
    """Downloader that fetches blobs through ocha-stratus (SAS from the
    DSCI_AZ_BLOB_{DEV,PROD}_SAS env vars) instead of a storage-account key.
    Keeps the hdx Retrieve interface: Retrieve.download_file() calls this with
    url/path plus the extra kwargs the pipeline passes (container, blob).
    """

    def download_file(
        self,
        url: str,
        container: Optional[str] = None,
        blob: Optional[str] = None,
        stage: str = "prod",
        **kwargs: Any,
    ) -> str:
        """Download a blob and store it at ``path`` (or folder/filename).

        Args:
            url (str): Blob name (kept for the Retrieve interface; ``blob`` wins).
            container (str): Container to download from.
            blob (str): Name of the blob to download. Defaults to ``url``.
            stage (str): "dev" or "prod" storage account. Defaults to "prod".
            **kwargs: folder / filename / path / overwrite / keep as in Download.

        Returns:
            str: Path of downloaded file
        """
        folder = kwargs.get("folder")
        filename = kwargs.get("filename")
        path = kwargs.get("path")
        overwrite = kwargs.get("overwrite", False)
        keep = kwargs.get("keep", False)
        blob = blob or url

        if not path:
            path = join(folder or gettempdir(), filename or basename(blob))
        if keep and exists(path) and not overwrite:
            logger.info(f"Keeping existing file {path}")
            return path

        makedirs(dirname(path) or ".", exist_ok=True)
        container_client = stratus.get_container_client(container, stage=stage)
        with open(path, "wb") as f:
            container_client.download_blob(blob).readinto(f)
        return path


def main(
    save: bool = False,
    use_saved: bool = False
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save: Save downloaded data. Defaults to False.
        use_saved: Use saved data. Defaults to False.

    Returns:
        None
    """
    with wheretostart_tempdir_batch(_LOOKUP) as info:
        folder = info["folder"]
        batch = info["batch"]
        configuration = Configuration.read()
        User.check_current_user_write_access("aer")
        with AzureBlobDownload() as downloader:
            retriever = Retrieve(
                downloader, folder, _SAVED_DATA_DIR, folder, save, use_saved
            )
            floodscan = Floodscan(configuration, retriever, folder)
            iso3s = floodscan.get_data()
            logger.info(
                f"Number of datasets to upload: {len(iso3s)}"
            )

            for _, nextdict in progress_storing_folder(
                info, iso3s, "iso3"
            ):
                iso3 = nextdict["iso3"]
                dataset = floodscan.generate_dataset(iso3)
                if dataset:
                    dataset.update_from_yaml()
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=batch,
                    )

            logger.info("Finished processing")


if __name__ == "__main__":
    print()
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=join("config", "project_configuration.yaml"),
    )
