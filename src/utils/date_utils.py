import argparse
import re
from datetime import datetime, timedelta
from zipfile import ZipFile


def extract_date(x):
    # Extract the date string in the format YYYYMMDD
    date_str = re.search(r"\d{8}", x).group()
    # Convert the extracted string to a date object
    return datetime.strptime(date_str, "%Y%m%d")


def date_to_run(date=None):
    if date:
        ret = datetime.strptime(date, "%Y-%m-%d")  # .date()
    else:
        ret = datetime.today() - timedelta(days=5)  # .date()
    return ret


def create_date_range(days, last_date):
    """
    Method to create a range of dates fof the past N days.

    Args:
        last_date: Which date to use as the end date.
        days (int): Number of the days before the last_date to generate range of.

    Returns:
        dates (list): List with the range dates
    """

    if days < 1:
        raise argparse.ArgumentTypeError("Days cannot be lower than 2.")

    date_range = []
    current_date = last_date
    while len(date_range) <= days:
        date_range.append(current_date)
        current_date -= timedelta(days=1)

    return date_range


def get_start_and_last_date_from_90_days_file(zipped_file):
    search_str = "([0-9]{4}[0-9]{2}[0-9]{2})"
    with ZipFile(zipped_file, "r") as zipobj:
        filenames = zipobj.namelist()
        newest = max(filenames)
        oldest = min(filenames)
        end_date_str = re.search(search_str, newest)
        start_date_str = re.search(search_str, oldest)
        return datetime.strptime(
            start_date_str[0], "%Y%m%d"
        ), datetime.strptime(end_date_str[0], "%Y%m%d")


def get_start_and_last_date_from_90_days(files):
    search_str = "([0-9]{4}[0-9]{2}[0-9]{2})"
    end_date_str = re.search(search_str, max(files))
    start_date_str = re.search(search_str, min(files))

    return datetime.strptime(start_date_str[0], "%Y%m%d"), datetime.strptime(
        end_date_str[0], "%Y%m%d"
    )
