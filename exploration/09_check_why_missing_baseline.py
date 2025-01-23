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

import pandas as pd

from src.utils import pg

# %% [markdown]
# trying to understand why values are missing in teh stage baseline files

# %%
df_doy_means = pg.fs_rolling_11_day_mean(
    mode="prod", admin_level=1, band="SFED"
)

df_last_90 = pg.fs_last_90_days(mode="prod", admin_level=1)
df_last_90_not_na = df_last_90[~df_last_90["value"].isna()]

missing_pcodes = df_last_90_not_na[
    ~df_last_90_not_na["pcode"].isin(df_doy_means["pcode"])
]["pcode"].unique()
missing_pcodes  # none

# %% [markdown]
# There are some Null/NA sfed_baeline values in the df_doy_means
# so below we check why. It's because those values are missing SFED raw
# and this is due to the raster stats methodology applied to small admin
# boundaries.
# we show this below:


# %%

# grab rows w/ missing sfed_baseline
df_missing_sfed_baseline = df_doy_means[df_doy_means["sfed_baseline"].isna()]

# do a count per country -- this is useful as we see it's in non HRP
# countries at this admi level
df_missing_sfed_baseline.groupby(["iso3"]).size().reset_index(name="count")

# specific pcodes
df_missing_sfed_baseline["pcode"].unique().tolist()
df_missing_sfed_baseline


# go directly to the DB and look at those problematic pcodes - we see
# they are all missing there SFED values (no raster stats available)
def query_missing_sfed_baseline():
    engine = pg.get_engine(mode="prod")
    iso3s = df_missing_sfed_baseline["pcode"].unique().tolist()
    iso3s_str = ", ".join([f"'{iso3}'" for iso3 in iso3s])
    query = f"""
    SELECT *
    FROM floodscan
    WHERE pcode IN ({iso3s_str}) # noqa: E222
    """
    return pd.read_sql(sql=query, con=engine)


missing_sfed_baseline_data = query_missing_sfed_baseline()
missing_sfed_baseline_data

unique_mean_values = missing_sfed_baseline_data["mean"].unique()
unique_mean_values
