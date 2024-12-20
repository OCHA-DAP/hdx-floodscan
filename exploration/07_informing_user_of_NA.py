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

# %% [markdown]
# The purpose of this notebook is to examine and discuss how we want to deal
# with `NA`/ `NAN` values in the data.
#
# Driver: Our understanding is that these are a result our zonal statistics
# methodology which uses raster resampling. In some cases the resampled
# resolution is too coarse for very small admin boundary units admins
# boundaries with no pixel centroids are present. These therefore have `NaN`
# values in the `value` column of our prod database. You can see more on this
# discussed in relations to this project
# [here](https://github.com/OCHA-DAP/ds-raster-stats/issues/28) and
# [here](https://github.com/OCHA-DAP/ds-raster-stats/issues/20).
#
# Anyways we can continue to discuss options in the issues above, but i wanted
# to provide this notebook so that we can see the extent of the problem and
# refer to it there.

import pandas as pd

# %%
from src.utils import pg
from src.utils import return_periods as rp

# %%
df_adm2 = pg.fs_last_90_days(mode="prod", admin_level=2, band="SFED")
df_adm1 = pg.fs_last_90_days(mode="prod", admin_level=1, band="SFED")

# %% [markdown]
# Here is the issue at the admin 2 level

# %%
rp.extract_nan_strata(df_adm2, by=["iso3", "pcode"])


# %% [markdown]
# And also the admin 1 level. However, I suppose that these are not HRP
# countries so not directly relevant/ an issue for this project at this stage.

# %%
rp.extract_nan_strata(df_adm1, by=["iso3", "pcode"])

# %% [markdown]
# ## Options:
#
# There are various options on how to deal with this which we can discuss more
# in the PR. Here are 2
#
# 1. We exclude them from the tabular excel data set  and include a metadata
# table that contains the admin missing admins either in the readme or in
# another tab. Then we need to put some sort of disclaimer.
# 2. We keep the admins in with blank SFED values and then include a disclaimer
#
# I like option 2 as it avoids both the need to update a table in the in readme
# as well as placing an outsized emphasis on this relatively smally issue.
# I think it will also make the ultimate disclaimer less wordy.
#
# ## Option 2 implemented below
#
# option 2 would be relatively straight forward to implement - and could be
# done like the below example. It's important to note that this step must
# occur before the admin labelling step so that these admins also get labelled.

# %%

df_current = pg.fs_last_90_days(mode="prod", admin_level=2, band="SFED")
df_yr_max = pg.fs_year_max(mode="prod", admin_level=2, band="SFED")

# %%
df_w_rps = rp.fs_add_rp(
    df=df_current, df_maxima=df_yr_max, by=["iso3", "pcode"]
)

# %%

df_sfed_missing = df_current[
    ~df_current[["iso3", "pcode"]]
    .apply(tuple, 1)
    .isin(df_w_rps[["iso3", "pcode"]].apply(tuple, 1))
]
df_sfed_missing

# %%
df_w_rps_complete = pd.concat([df_w_rps, df_sfed_missing], ignore_index=True)
df_w_rps_complete
