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
