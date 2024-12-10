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

import pandas as pd

# %%
# %matplotlib inline
# %load_ext autoreload
# %autoreload 2
from src.utils import pg

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
# %%
