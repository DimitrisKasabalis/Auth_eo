import calendar
from datetime import datetime, date as dt_date
from math import ceil
from typing import Literal, Tuple

from dateutil.relativedelta import relativedelta


# Dekad Code picked from
# https://github.com/TUW-GEO/cadati/blob/master/src/cadati/dekad.py
# """
# This module provides functions for date manipulation on a dekadal basis.
# A dekad is defined as days 1-10, 11-20 and 21-last day of a month.
# Or in numbered dekads:
# 1: day 1-10
# 2: day 11-20
# 3: day 21-last
# """

def dekad_end_date(date: dt_date):
    """Checks the dekad of a date and returns the dekad date.
    Parameters
    ----------
    date : datetime
        Date to check.
    Returns
    -------
    new_date : datetime
        Date of the dekad.
    """

    if date.day < 11:
        dekad = 10
    elif date.day > 10 and date.day < 21:
        dekad = 20
    else:
        dekad = calendar.monthrange(date.year, date.month)[1]

    new_date = datetime(year=date.year, month=date.month, day=dekad)

    return new_date


def dekad_startdate(dt_in: dt_date):
    """
    dekadal startdate that a date falls in
    Parameters
    ----------
    run_dt: datetime.datetime
    Returns
    -------
    startdate: datetime.datetime
        startdate of dekad
    """
    if dt_in.day <= 10:
        startdate = dt_date(dt_in.year, dt_in.month, 1)
    if 11 <= dt_in.day <= 20:
        startdate = dt_date(dt_in.year, dt_in.month, 11)
    if dt_in.day >= 21:
        startdate = dt_date(dt_in.year, dt_in.month, 21)

    # noinspection PyUnboundLocalVariable
    return startdate


def dekad2start_end_days(year, month, dekad) -> Tuple[dt_date, dt_date]:
    """Gets the day of a dekad.
    Parameters
    ----------
    year : int
        Year of the date.
    month : int
        Month of the date.
    dekad : int
        Dekad of the date.
    Returns
    -------
    day : int
        Day value for the dekad.
    """

    if dekad == 1:
        return dt_date(year, month, 1), dt_date(year, month, 10)
    elif dekad == 2:
        return dt_date(year, month, 11), dt_date(year, month, 20)
    elif dekad == 3:
        last_day_of_month = calendar.monthrange(year, month)[1]
        return dt_date(year, month, 21), dt_date(year, month, last_day_of_month)


def runningdekad2date(year: int, rdekad: int) -> (dt_date, dt_date):
    """Gets the date of the running dekad of a spacifc year.
    Parameters
    ----------
    year : int
        Year of the date.
    rdekad : int
        Running dekad of the date.
    Returns
    -------
    (datetime.date,datetime.date)
        Start/End Date value for the running dekad.
    """
    if not 0 < rdekad < 37:
        raise ValueError('running dekad must be between in 1..36')
    month = int(ceil(rdekad / 3.))
    dekad = rdekad - month * 3 + 3
    start_day, end_date = dekad2start_end_days(year, month, dekad)

    return start_day, end_date


def day2dekad(day):
    """Returns the dekad of a day.
    Parameters
    ----------
    day : int
        Day of the date.
    Returns
    -------
    dekad : int
        Number of the dekad in a month.
    """

    if day < 11:
        dekad = 1
    elif day > 10 and day < 21:
        dekad = 2
    else:
        dekad = 3

    return dekad
