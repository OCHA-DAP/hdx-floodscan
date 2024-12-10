import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

AZURE_DB_PW_PROD = os.getenv("AZURE_DB_PW_PROD")
AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")


def get_engine(mode):
    if mode == "prod":
        pw = AZURE_DB_PW_PROD
    else:
        pw = AZURE_DB_PW_DEV

    url = f"postgresql+psycopg2://chdadmin:{pw}@chd-rasterstats-{mode}.postgres.database.azure.com/postgres"  # noqa: E501 E231
    return create_engine(url)


def fs_year_max(mode, admin_level, band="SFED"):
    engine = get_engine(mode)

    query_yr_max = f"""
        SELECT iso3, pcode, DATE_TRUNC('year', valid_date) AS year_date, MAX(mean) AS value
        FROM floodscan
        WHERE adm_level = {admin_level}
          AND band = '{band}'
          AND valid_date <= '2023-12-31'
        GROUP BY iso3, pcode, year_date
    """
    return pd.read_sql(sql=query_yr_max, con=engine)


def fs_rolling_11_day_mean(mode, admin_level, band="SFED"):
    engine = get_engine(mode)

    query_rolling_mean = f"""
        WITH filtered_data AS (
            SELECT iso3, pcode, valid_date, mean
            FROM floodscan
            WHERE adm_level = {admin_level}
                AND band = '{band}'
                AND valid_date >= DATE_TRUNC('year', NOW()) - INTERVAL '10 years'
                AND valid_date < DATE_TRUNC('year', NOW())
        ),
        rolling_mean AS (
            SELECT iso3, pcode, valid_date,
                    AVG(mean) OVER (PARTITION BY iso3, pcode ORDER BY valid_date
                                    ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS rolling_mean
            FROM filtered_data
        ),
        doy_mean AS (
            SELECT iso3, pcode, EXTRACT(DOY FROM valid_date) AS doy,
                    AVG(rolling_mean) AS SFED_BASELINE
            FROM rolling_mean
            GROUP BY iso3, pcode, doy
        )
        SELECT * FROM doy_mean
    """  # noqa: E202 E231
    return pd.read_sql(sql=query_rolling_mean, con=engine)


def fs_last_90_days(mode, admin_level, band="SFED"):
    engine = get_engine(mode)

    query_last_90_days = f"""
    SELECT iso3, pcode, valid_date, mean AS value
    FROM floodscan
    WHERE adm_level = {admin_level}
      AND band = '{band}'
      AND valid_date >= NOW() - INTERVAL '90 days'
    """
    return pd.read_sql(sql=query_last_90_days, con=engine)


# this one is experimental - may mess around and see how it works on prod
def create_yr_max_view(mode, admin_level, band):
    engine = get_engine(mode)
    with engine.connect() as conn:
        query = f"""
            CREATE OR REPLACE VIEW floodscan_yearly_max AS
            SELECT iso3, pcode, year_date, MAX(mean) AS value
            FROM (
                SELECT iso3, pcode, adm_level, valid_date, band, mean,
                       DATE_TRUNC('year', valid_date) AS year_date
                FROM floodscan
                WHERE adm_level = {admin_level}
                  AND band = '{band}'
                  AND valid_date <= '2023-12-31'
            ) AS filtered_floodscan
            GROUP BY iso3, pcode, year_date
        """  # noqa E231 E202
        conn.execute(query)
