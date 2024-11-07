import datetime
import re


def extract_date(x):
    # Extract the date string in the format YYYYMMDD
    date_str = re.search(r"\d{8}", x).group()
    # Convert the extracted string to a date object
    return datetime.datetime.strptime(date_str, "%Y%m%d")


def date_to_run(date=None):
    if date:
        ret = datetime.datetime.strptime(date, "%Y-%m-%d")  # .date()
    else:
        ret = datetime.date.today() - datetime.timedelta(days=5)  # .date()
    return ret
