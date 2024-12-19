# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: hdx-floodscan
#     language: python
#     name: hdx-floodscan
# ---

# %%
# %matplotlib inline
# %load_ext autoreload
# %autoreload 2

from io import BytesIO

# %%
import pandas as pd

from src.utils import cloud_utils as blob
from src.utils import pg  # postgres

# %% [markdown]
# The final data sets will need to be labelled with both admin codes and admin
# names. Currently the labels can only be found in the `prod` `polygon` table.
#
# Given the current format of the polygon table we can label the lowest level
# admin unit. For example it's easy to perform joins to label the admin 2 level
# floodscan data with the polygons table. However, it is not easy to then add
# on the admin 1 level data labels which should be in the final tabular
# data set.
#
# Out of necessity in the past, I have performed the necessary regex wrangling
# gymnastics to do this in `R`, but I don't think this type of complexity is
# beneficial for the formal data pipeline. Rather, it would be better address
# on the DB side data structure.
#
# Nonetheless, the purpose of this notebook is to:
# 1. highlight the above
# 2. Show the joins generally necessary to label the data
# 3. highlight some other issues in the DB that have been raised as issues,
# but affect this step specifically.
# 4. and finally highlight the work-around that will use a `parquet` file
# stored on the blob to implement the hdx-floodscan tabular labelling.
# %%


engine = pg.get_engine(mode="prod")


poly_query = """
SELECT pcode, iso3, adm_level, name, name_language
FROM polygon
WHERE adm_level = 2
"""
adm2_labels = pd.read_sql_query(poly_query, engine)

adm2_labels


# %%
df_adm2_90d = pg.fs_last_90_days(mode="prod", admin_level=2, band="SFED")

# %%
df_adm2_labelled = df_adm2_90d.merge(
    adm2_labels, on=["iso3", "pcode"], how="left"
)

# %% [markdown]
# above we've merged the labels on to the main data set, but below we can see
# the problematic `iso3` `pcode` combos that do
# not have labels in the polygons data set. It seems this issue affects all
# admin levels to different degrees
#
# - [Missing countries with duplicated adm0 SHPs](https://github.com/OCHA-DAP/ds-raster-stats/issues/16) # noqa: E501
# - [discrepancy in admin 2 boundaries - polygon vs floodscan](https://github.com/OCHA-DAP/ds-raster-stats/issues/19) # noqa: E501

# %%
df_adm2_labelled
df_no_match = df_adm2_labelled[df_adm2_labelled["name"].isna()]
iso3_pcodes_no_match = df_no_match[["iso3", "pcode"]].drop_duplicates()
iso3_pcodes_no_match
# %% [markdown]
# Ok we recently generated a work-around using parquet see
# [PR here](https://github.com/OCHA-DAP/ds-raster-stats/pull/34)

# %%
pc = blob.get_container_client(mode="dev", container_name="polygon")
blob_name = "admin_lookup.parquet"
blob_client = pc.get_blob_client(blob_name)
blob_data = blob_client.download_blob().readall()
df_parquet_labels = pd.read_parquet(BytesIO(blob_data))

# %%
df_labels_adm2 = df_parquet_labels[df_parquet_labels.ADM_LEVEL == 2]

df_fs_labelled = pd.merge(
    df_adm2_90d,
    df_labels_adm2,
    left_on=["iso3", "pcode"],
    right_on=["ISO3", "ADM2_PCODE"],
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

# %% [markdown]
# Check below shows that the join worked with no missing admin strata

# %%
df_no_match = df_fs_labelled_subset[df_fs_labelled_subset["ADM2_PCODE"].isna()]

df_no_match

# %% [markdown]
# I don't have contextual knowledge to check all the admin names, but we can
# quickly look at country level.  I'd say some of these labels while perhaps
# standard by some measurement are not ideal. I'd say we can remove the
# `"(le)$"` and `"(the)$"`  stings as part of the hdx pipeline.

# %%
# Count the number of records per Country_Name
df_country_n = (
    df_fs_labelled_subset.groupby("ADM0_NAME")
    .size()
    .reset_index(name="counts")
)

# Display the result
df_country_n
