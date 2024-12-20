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

# %%
import numpy as np

from src.utils import pg
from src.utils import return_periods as rp

# %%
df_current = pg.fs_last_90_days(mode="prod", admin_level=2, band="SFED")
df_yr_max = pg.fs_year_max(mode="prod", admin_level=2, band="SFED")

# %%
df_w_rps = rp.fs_add_rp(
    df=df_current, df_maxima=df_yr_max, by=["iso3", "pcode"]
)

# %% [markdown]
# we can have a metadata tab + table that explains why the admin below are not
# included

# %%
rp.extract_nan_strata(df_current, by=["iso3", "pcode"])

# %%

max_rp = df_w_rps["RP"].max()
max_rp = df_w_rps[df_w_rps["RP"] != np.inf]["RP"].max()
print(max_rp)

# %% [markdown]
# Good no longer any NA/NAN values being produced and instead they are
# correctly set as `Inf`
#

# %%
df_w_rps[df_w_rps.RP.isna()]

# %%
df_w_rps[np.isinf(df_w_rps["RP"])]
