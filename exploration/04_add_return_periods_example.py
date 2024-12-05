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
# ---

# %%
# %matplotlib inline
# %load_ext autoreload
# %autoreload 2

# %%
from src.utils import pg
from src.utils import return_periods as rp

# import pandas as pd
# import numpy as np
# from scipy.interpolate import interp1d

# %%
df_current = pg.fs_last_90_days(mode="prod", admin_level=2, band="SFED")
df_yr_max = pg.fs_year_max(mode="prod", admin_level=2, band="SFED")

# %%
df_w_rps = rp.fs_add_rp(
    df=df_current, df_maxima=df_yr_max, by=["iso3", "pcode"]
)
