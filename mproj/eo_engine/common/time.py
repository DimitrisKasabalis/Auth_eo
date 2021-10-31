import calendar
from datetime import datetime
from math import ceil
from typing import Literal

from dateutil.relativedelta import relativedelta


def monthly_dekad_to_yearly_dekad(dekad: Literal[1, 2, 3], month: int) -> str:
    dekad = int(dekad)
    month = int(month)
    if 0 > month or month > 12:
        raise AttributeError('month should be between 1 and 12')
    if dekad not in (1, 2, 3):
        raise AttributeError('dekad should be 1,2 or 3')
    return str(dekad * month).zfill(2)


def yearly_dekad_to_wapor_range(token: str) -> (datetime, datetime):
    """1904 -> 2019, month 2, first dekad of the month"""

    year = int(token[:2]) + 2000
    yearly_dekad = int(token[2:])

    month = int(ceil(yearly_dekad / 3.))
    running_dekad = yearly_dekad - month * 3 + 3
    if running_dekad == 0:
        start_date = 1
        end_date = 10
    elif running_dekad == 1:
        start_date = 11
        end_date = 20
    else:
        start_date = 21
        end_date = calendar.monthrange(year, month)[1]

    return datetime(year=year, month=month, day=start_date), \
           datetime(year=year, month=month, day=end_date) + relativedelta(days=1)
