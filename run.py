#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import base64
import hashlib
import hmac
import logging
from datetime import datetime
from os.path import exists, expanduser, join

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

"""Facade to simplify project setup that calls project main function with kwargs"""

from typing import Any, Callable, Optional  # noqa: F401

from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)

lookup = "floodscan"
updated_by_script = "HDX Scraper: FloodScan"


class AzureBlobDownload(Download):
    def download_file(
        self,
        url: str,
        account: str,
        container: str,
        key: str,
        blob: None,
        **kwargs: Any,
    ) -> str:
        """Download file from blob storage and store in provided folder or temporary
        folder if no folder supplied.

        Args:
            url (str): URL for the exact blob location
            account (str): Storage account to access the blob
            container (str): Container to download from
            key (str): Key to access the blob
            blob (str): Name of the blob to be downloaded. If empty, then it is assumed to download the whole container.
            **kwargs: See below
            folder (str): Folder to download it to. Defaults to temporary folder.
            filename (str): Filename to use for downloaded file. Defaults to deriving from url.
            path (str): Full path to use for downloaded file instead of folder and filename.
            overwrite (bool): Whether to overwrite existing file. Defaults to False.
            keep (bool): Whether to keep already downloaded file. Defaults to False.
            post (bool): Whether to use POST instead of GET. Defaults to False.
            parameters (Dict): Parameters to pass. Defaults to None.
            timeout (float): Timeout for connecting to URL. Defaults to None (no timeout).
            headers (Dict): Headers to pass. Defaults to None.
            encoding (str): Encoding to use for text response. Defaults to None (best guess).

        Returns:
            str: Path of downloaded file
        """
        folder = kwargs.get("folder")
        filename = kwargs.get("filename")
        path = kwargs.get("path")
        overwrite = kwargs.get("overwrite", False)
        keep = kwargs.get("keep", False)

        request_time = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        api_version = "2018-03-28"
        parameters = {
            "verb": "GET",
            "Content-Encoding": "",
            "Content-Language": "",
            "Content-Length": "",
            "Content-MD5": "",
            "Content-Type": "",
            "Date": "",
            "If-Modified-Since": "",
            "If-Match": "",
            "If-None-Match": "",
            "If-Unmodified-Since": "",
            "Range": "",
            "CanonicalizedHeaders": "x-ms-date:"
            + request_time
            + "\nx-ms-version:"
            + api_version
            + "\n",
            "CanonicalizedResource": "/"
            + account
            + "/"
            + container
            + "/"
            + blob,
        }

        signature = (
            parameters["verb"]
            + "\n"
            + parameters["Content-Encoding"]
            + "\n"
            + parameters["Content-Language"]
            + "\n"
            + parameters["Content-Length"]
            + "\n"
            + parameters["Content-MD5"]
            + "\n"
            + parameters["Content-Type"]
            + "\n"
            + parameters["Date"]
            + "\n"
            + parameters["If-Modified-Since"]
            + "\n"
            + parameters["If-Match"]
            + "\n"
            + parameters["If-None-Match"]
            + "\n"
            + parameters["If-Unmodified-Since"]
            + "\n"
            + parameters["Range"]
            + "\n"
            + parameters["CanonicalizedHeaders"]
            + parameters["CanonicalizedResource"]
        )

        signed_string = base64.b64encode(
            hmac.new(
                base64.b64decode(key),
                msg=signature.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
        ).decode()

        headers = {
            "x-ms-date": request_time,
            "x-ms-version": api_version,
            "Authorization": ("SharedKey " + account + ":" + signed_string),
        }

        url = (
            "https://"
            + account
            + ".blob.core.windows.net/"
            + container
            + "/"
            + blob
        )

        if keep and exists(url):
            print(f"The blob URL exists: {url}")
            return path
        self.setup(
            url=url,
            stream=True,
            post=kwargs.get("post", False),
            parameters=kwargs.get("parameters"),
            timeout=kwargs.get("timeout"),
            headers=headers,
            encoding=kwargs.get("encoding"),
        )
        return self.stream_path(
            path, f"Download of {url} failed in retrieval of stream!"
        )


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
                                hxl_update=False,
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
