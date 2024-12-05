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