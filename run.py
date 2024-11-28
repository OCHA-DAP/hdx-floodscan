#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join, exists
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
from hdx.data.hdxobject import HDXError
from datetime import datetime
import hmac
import hashlib
import base64
from hdx_floodscan import HDXFloodscan

"""Facade to simplify project setup that calls project main function with kwargs"""

import sys
from inspect import getdoc
from typing import Any, Callable, Optional  # noqa: F401
import defopt
from makefun import with_signature
from hdx.api import __version__
from hdx.api.configuration import Configuration
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)

lookup = "hdx-floodscan"
updated_by_script = "HDX Scraper: HDX FloodScan"


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

        request_time = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        api_version = '2018-03-28'
        parameters = {
            'verb': 'GET',
            'Content-Encoding': '',
            'Content-Language': '',
            'Content-Length': '',
            'Content-MD5': '',
            'Content-Type': '',
            'Date': '',
            'If-Modified-Since': '',
            'If-Match': '',
            'If-None-Match': '',
            'If-Unmodified-Since': '',
            'Range': '',
            'CanonicalizedHeaders': 'x-ms-date:' + request_time + '\nx-ms-version:' + api_version + '\n',
            'CanonicalizedResource': '/' + account + '/' + container + '/' + blob
        }

        signature = (parameters['verb'] + '\n'
                          + parameters['Content-Encoding'] + '\n'
                          + parameters['Content-Language'] + '\n'
                          + parameters['Content-Length'] + '\n'
                          + parameters['Content-MD5'] + '\n'
                          + parameters['Content-Type'] + '\n'
                          + parameters['Date'] + '\n'
                          + parameters['If-Modified-Since'] + '\n'
                          + parameters['If-Match'] + '\n'
                          + parameters['If-None-Match'] + '\n'
                          + parameters['If-Unmodified-Since'] + '\n'
                          + parameters['Range'] + '\n'
                          + parameters['CanonicalizedHeaders']
                          + parameters['CanonicalizedResource'])

        signed_string = base64.b64encode(hmac.new(base64.b64decode(key), msg=signature.encode('utf-8'),
                                                  digestmod=hashlib.sha256).digest()).decode()

        headers = {
            'x-ms-date': request_time,
            'x-ms-version': api_version,
            'Authorization': ('SharedKey ' + account + ':' + signed_string)
        }

        url = ('https://' + account + '.blob.core.windows.net/' + container + '/' + blob)

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

 # TODO Improve logic around facade
def facade(projectmainfn: Callable[[Any], None], **kwargs: Any):
    """Facade to simplify project setup that calls project main function. It infers
    command line arguments from the passed in function using defopt. The function passed
    in should have type hints and a docstring from which to infer the command line
    arguments. Any **kwargs given will be merged with command line arguments, with the
    command line arguments taking precedence.

    Args:
        projectmainfn ((Any) -> None): main function of project
        **kwargs: Configuration parameters to pass to HDX Configuration & other parameters to pass to main function

    Returns:
        None
    """

    #
    # Setting up configuration
    #

    parsed_main_doc = defopt._parse_docstring(getdoc(projectmainfn))
    main_doc = [f"{parsed_main_doc.first_line}\n\nArgs:"]
    no_main_params = len(parsed_main_doc.params)
    for param_name, param_info in parsed_main_doc.params.items():
        main_doc.append(
            f"\n    {param_name} ({param_info.type}): {param_info.text}"
        )
    create_config_doc = getdoc(Configuration.create)
    kwargs_index = create_config_doc.index("**kwargs")
    kwargs_index = create_config_doc.index("\n", kwargs_index)
    args_doc = create_config_doc[kwargs_index:]
    main_doc.append(args_doc)
    main_doc = "".join(main_doc)

    main_sig = defopt.signature(projectmainfn)
    param_names = []
    for param in main_sig.parameters.values():
        param_names.append(str(param))

    parsed_main_doc = defopt._parse_docstring(main_doc)
    main_doc = [f"{parsed_main_doc.first_line}\n\nArgs:"]
    count = 0
    for param_name, param_info in parsed_main_doc.params.items():
        param_type = param_info.type
        if param_type == "dict":
            continue
        if count < no_main_params:
            count += 1
        else:
            if param_type == "str":
                param_type = "Optional[str]"
                default = None
            elif param_type == "bool":
                default = False
            else:
                raise ValueError(
                    "Configuration.create has new parameter with unknown type!"
                )
            param_names.append(f"{param_name}: {param_type} = {default}")
        main_doc.append(
            f"\n    {param_name} ({param_type}): {param_info.text}"
        )
    main_doc = "".join(main_doc)

    projectmainname = projectmainfn.__name__
    main_sig = f"{projectmainname}({','.join(param_names)})"

    argv = sys.argv[1:]
    for key in kwargs:
        name = f"--{key.replace('_', '-')}"
        if name not in argv:
            argv.append(name)
            argv.append(kwargs[key])

    @with_signature(main_sig)
    def gen_func(*args, **kwargs):
        """docstring"""
        site_url = Configuration._create(*args, **kwargs)
        logger.info("--------------------------------------------------")
        logger.info(f"> Using HDX Python API Library {__version__}")
        logger.info(f"> HDX Site: {site_url}")

    gen_func.__doc__ = main_doc

    configuration_create = defopt.bind(gen_func, argv=argv, cli_options="all")
    main_func, _ = defopt.bind_known(
        projectmainfn, argv=argv, cli_options="all"
    )

    configuration_create()
    UserAgent.user_agent = Configuration.read().user_agent
    main_func()


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
                floodscan = HDXFloodscan(configuration, retriever, folder, errors)
                dataset_names = floodscan.get_data()
                logger.info(f"Number of datasets to upload: {len(dataset_names)}")

                for _, nextdict in progress_storing_folder(info, dataset_names, "name"):
                    dataset_name = nextdict["name"]
                    dataset = floodscan.generate_dataset_and_showcase(dataset_name=dataset_name)
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
                                ignore_fields=["resource:description", "extras"],
                            )
                        except HDXError as err:
                            errors.add(f"Could not upload {dataset_name}: {err}")
                            continue



if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    facade(
        main,
        user_agent_config_yaml="/Workspace/Shared/init-scripts/.useragents.yaml",
        hdx_config_yaml="/Workspace/Shared/init-scripts/.hdx_configuration.yaml",
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),

    )