#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import (
    progress_storing_folder,
    wheretostart_tempdir_batch,
)

from floodscan import Floodscan

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-floodscan"
_SAVED_DATA_DIR = "saved_data"
_UPDATED_BY_SCRIPT = "HDX Scraper: FloodScan"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save: Save downloaded data. Defaults to False.
        use_saved: Use saved data. Defaults to False.

    Returns:
        None
    """
    with wheretostart_tempdir_batch(_LOOKUP) as info:
        tempdir = info["folder"]
        batch = info["batch"]
        configuration = Configuration.read()
        floodscan = Floodscan(configuration, save, use_saved, tempdir, _SAVED_DATA_DIR)
        dataset_names = floodscan.get_data()
        logger.info(
            f"Number of datasets to upload: {len(dataset_names)}"
        )

        for _, nextdict in progress_storing_folder(
            info, dataset_names, "name"
        ):
            dataset_name = nextdict["name"]
            dataset = floodscan.generate_dataset(
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
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=batch,
                    )
                except HDXError as err:
                    logger.error(
                        f"Could not upload {dataset_name}: {err}"
                    )
                    continue


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=join("config", "project_configuration.yaml"),
    )
