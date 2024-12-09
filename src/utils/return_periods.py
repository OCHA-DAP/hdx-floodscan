import numpy as np
import pandas as pd
from lmoments3 import distr, lmom_ratios
from scipy.interpolate import interp1d
from scipy.stats import norm, pearson3, skew

# Empirical RP Functions


def fs_add_rp(df, df_maxima, by):
    df_nans, df_maxima_nans = [
        extract_nan_strata(x, by=by) for x in [df, df_maxima]
    ]

    df_filt = df[
        ~df[by].apply(tuple, axis=1).isin(df_nans.apply(tuple, axis=1))
    ]
    df_maxima_filt = df_maxima[
        ~df_maxima[by]
        .apply(tuple, axis=1)
        .isin(df_maxima_nans.apply(tuple, axis=1))
    ]

    df_rps = (
        df_maxima_filt.groupby(by, group_keys=True)
        .apply(empirical_rp, include_groups=False)
        .reset_index()
        .drop(columns=["level_2"])
    )

    interp_funcs = interpolation_functions_by(
        df=df_rps, rp="RP", value="value", by=by
    )

    df_filt.loc[:, "RP"] = df_filt.apply(
        lambda row: apply_interp(row, interp_funcs, by=by),
        axis=1,
    ).astype(float)

    df_filt.loc[:, "RP"] = clean_rps(df_filt["RP"], decimals=3, upper=10)

    return df_filt


def extract_nan_strata(df, by=["iso3", "pcode"]):
    return df[df["value"].isna()][by].drop_duplicates()


def interpolation_functions_by(df, rp, value, by=["iso3", "pcode"]):
    """
    Generate interpolation functions for each group in the DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing the data.
    rp (str): The column name in the DataFrame representing the
    return period values.
    value (str): The column name in the DataFrame representing the values
    to interpolate.
    by (list of str, optional): The list of column names to group by.
    Default is ["iso3", "pcode"].

    Returns:
    dict: A dictionary where keys are tuples of group values and values are
    interpolation functions.
    """
    interp_funcs = (
        df.groupby(by)
        .apply(
            lambda group: interp1d(
                group[value],
                group[rp],
                bounds_error=False,
                fill_value=(1, np.nan),
            ),
            include_groups=False,
        )
        .to_dict()
    )

    return interp_funcs


def apply_interp(row, interp_dict, by=["iso3", "pcode"]):
    try:
        key = tuple(row[col] for col in by)
        interp_func = interp_dict[key]
        return interp_func(row["value"])
    except KeyError:
        return np.nan


def clean_rps(x, decimals=3, upper=10):
    x_round = x.round(3)
    x_round[x_round > upper] = np.inf
    return x_round


def empirical_rp(group):
    if group["value"].isna().any():
        raise ValueError("The value column contains NaN values")

    group["RANK"] = group["value"].rank(ascending=False)
    group["RP"] = (len(group) + 1) / group["RANK"]
    return group


# LP3 Functions


def lp3_params(x, est_method="lmoments"):
    x = np.asarray(x)
    x[x == 0] = np.min(x[x != 0])
    x_sorted = np.sort(x)
    x_log = np.log10(x_sorted)

    if est_method == "lmoments":
        lmoments = lmom_ratios(x_log, nmom=4)
        params = distr.pe3.lmom_fit(x_log, lmoments)  # , lmom_ratios=lmom
    elif est_method == "scipy":
        params = pearson3.fit(x_log, method="MM")
    elif est_method == "usgs":
        params = {
            "mu": np.mean(x_log),
            "sd": np.std(x_log, ddof=1),
            "g": skew(x_log),
        }
    else:
        raise ValueError(
            "Invalid est_method specified. Choose 'usgs','lmoments' or 'scipy'."  # noqa: E501
        )
    return params


def lp3_params_all(value):
    methods = ["usgs", "lmoments", "scipy"]
    params = {method: lp3_params(value, method) for method in methods}
    return pd.Series(params)


def lp3_rp(x, params, est_method="lmoments"):
    x = np.asarray(x)
    x_sorted = np.sort(x)
    x_log = np.log10(x_sorted)

    if est_method == "scipy":
        # Calculate the CDF for the log-transformed data
        p_lte = pearson3.cdf(x_log, *params)

    elif est_method == "lmoments":
        p_lte = distr.pe3.cdf(x_log, **params)

    elif est_method == "usgs":
        g = params["g"]
        mu = params["mu"]
        stdev = params["sd"]
        # Step 1: From quantiles_rp to y_fit_rp
        y_fit_rp = x_log

        # Step 2: From y_fit_rp to k_rp
        k_rp = (y_fit_rp - mu) / stdev

        # Step 3: From k_rp to q_rp_norm
        q_rp_norm = (g / 6) + ((k_rp * g / 2 + 1) ** (1 / 3) - 1) * 6 / g

        # Step 4: From q_rp_norm to rp_exceedance
        p_lte = norm.cdf(q_rp_norm, loc=0, scale=1)

    else:
        raise ValueError(
            "Invalid package specified. Choose 'distr' , 'usgs',or 'scipy'."
        )

    # Calculate the return periods
    p_gte = 1 - p_lte
    rp = 1 / p_gte

    return rp


def lp3_rv(rp, params, est_method="usgs"):
    """
    Calculate return values for given return periods using the
    Log-Pearson Type III distribution.

    Parameters:
    rp (list or array-like): List of return periods.
    params (dict or tuple): Parameters for the distribution.
        - For 'usgs' method, a dictionary with keys 'g' (skewness),
          'mu' (mean), and 'sd' (standard deviation).
        - For 'lmoments' method, a dictionary with parameters for the
          Log Pearson Type III distribution.
        - For 'scipy' method, a tuple with parameters for the
          Log Pearson Type III distribution.
    est_method (str, optional): Method to estimate the return values.
      Options are 'usgs', 'lmoments', or 'scipy'. Default is 'usgs'.

    Returns:
    numpy.ndarray: Return values corresponding to the given return periods.

    Raises:
    ValueError: If an invalid estimation method is provided.
    """
    est_method = est_method.lower()
    if est_method not in ["usgs", "lmoments", "scipy"]:
        raise ValueError(
            "Invalid method. Choose 'usgs' or 'lmoments' or 'scipy'."
        )
    rp_exceedance = [1 / rp for rp in rp]

    if est_method == "usgs":
        g = params["g"]
        mu = params["mu"]
        stdev = params["sd"]

        q_rp_norm = norm.ppf(
            1 - np.array(rp_exceedance), loc=0, scale=1
        )  # Normal quantiles
        k_rp = (2 / g) * (
            ((q_rp_norm - (g / 6)) * (g / 6) + 1) ** 3 - 1
        )  # Skewness adjustment
        y_fit_rp = mu + k_rp * stdev  # Fitted values for return periods
        ret = 10 ** (y_fit_rp)
        # return return_value_lp3_usgs(x, rp)
    elif est_method == "lmoments":
        value_log = distr.pe3.ppf(1 - np.array(rp_exceedance), **params)
        ret = 10 ** (value_log)
    elif est_method == "scipy":
        value_log = pearson3.ppf(1 - np.array(rp_exceedance), *params)
        ret = 10 ** (value_log)

    return ret
