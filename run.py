#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os import makedirs
from os.path import basename, dirname, exists, expanduser, join
from tempfile import gettempdir

import ocha_stratus as stratus

from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import (
    progress_storing_folder,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from floodscan import Floodscan

from typing import Any, Callable, Optional  # noqa: F401

from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)

lookup = "floodscan"
updated_by_script = "HDX Scraper: FloodScan"


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


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX"""
    with ErrorsOnExit() as errors:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            with AzureBlobDownload() as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                folder = info["folder"]
                batch = info["batch"]
                configuration = Configuration.read()
                floodscan = Floodscan(configuration, retriever, folder, errors)
                dataset_names = floodscan.get_data()
                logger.info(
                    f"Number of datasets to upload: {len(dataset_names)}"
                )

                for _, nextdict in progress_storing_folder(
                    info, dataset_names, "name"
                ):
                    dataset_name = nextdict["name"]
                    dataset = floodscan.generate_dataset_and_showcase(
                        dataset_name=dataset_name
                    )
                    if dataset:
                        dataset.update_from_yaml()
                        dataset["notes"] = dataset["notes"].replace(
                            "\n", "  \n"
                        )  # ensure markdown has line breaks
                        try:
                            dataset.create_in_hdx(
                                remove_additional_resources=True,
                                updated_by_script=updated_by_script,
                                batch=batch,
                                ignore_fields=[
                                    "resource:description",
                                    "extras",
                                ],
                            )
                        except HDXError as err:
                            errors.add(
                                f"Could not upload {dataset_name}: {err}"
                            )
                            continue


if __name__ == "__main__":
    print()
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
    )
