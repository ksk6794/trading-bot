from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Union

from numpy import float64
import dateparser
import pytz


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds

    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/

    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def remove_exponent(d: Decimal):
    """Remove exponent and trailing zeros.
    https://docs.python.org/2/library/decimal.html#decimal-faq
    >>> remove_exponent(Decimal('5E+3'))
    Decimal('5000')
    """
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()


def format_decimal(d: Decimal, decimal_places=3):
    places = Decimal('0.1') ** decimal_places
    return d.quantize(exp=Decimal(places), rounding=ROUND_DOWN).normalize()


def to_decimal(value: Union[float64, str], digits: int = 2):
    return round(Decimal(value), digits)


def to_decimal_places(d: Decimal, decimal_places: Decimal):
    return round(d / decimal_places) * decimal_places
