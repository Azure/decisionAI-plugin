from dateutil import parser, tz
import datetime
import dateutil

from .constant import MINT_IN_SECONDS, HOUR_IN_SECONDS, DAY_IN_SECONDS
from .gran import Gran

DT_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
DT_FILENAME_FORMAT = '%Y-%m-%dT_%H_%M_%SZ'

def convert_freq(gran, custom_in_seconds):
    if gran == Gran.Yearly:
        return 'Y'
    if gran == Gran.Monthly:
        return 'M'
    if gran == Gran.Weekly:
        return 'W'
    if gran == Gran.Daily:
        return 'D'
    if gran == Gran.Hourly:
        return 'H'
    if gran == Gran.Minutely:
        return 'T'
    if gran == Gran.Secondly:
        return 'S'

    return '{}S'.format(custom_in_seconds)


def str_to_dt(s):
    return parser.parse(s).replace(tzinfo=tz.UTC)

def dt_to_str(dt):
    return dt.strftime(DT_FORMAT)

def dt_to_str_file_name(dt):
    return dt.strftime(DT_FILENAME_FORMAT)

def get_diff(start, graninfo, end):
    (gran_str, custom_in_seconds) = graninfo
    delta = dateutil.relativedelta.relativedelta(end, start)
    if gran_str == 'Daily':
        diff = (end - start).total_seconds() / DAY_IN_SECONDS
    elif gran_str == 'Weekly':
        diff = (end - start).total_seconds() / (DAY_IN_SECONDS * 7)
    elif gran_str == 'Monthly':
        diff = delta.years * 12 + delta.months
    elif gran_str == 'Yearly':
        diff = delta.years
    elif gran_str == 'Hourly':
        diff = (end - start).total_seconds() / HOUR_IN_SECONDS
    elif gran_str == 'Minutely':
        diff = (end - start).total_seconds() / MINT_IN_SECONDS
    elif gran_str == 'Secondly':
        diff = (end - start).total_seconds()
    elif gran_str == 'Custom':
        diff = (end - start).total_seconds() / custom_in_seconds
    else:
        raise Exception('Granularity not supported: {}|{}'.format(*graninfo))

    return int(diff)


def get_time_offset(timestamp, graninfo, offset):
    (gran_str, custom_in_seconds) = graninfo

    if gran_str == 'Daily':
        return timestamp + datetime.timedelta(days=offset)
    elif gran_str == 'Weekly':
        return timestamp + datetime.timedelta(weeks=offset)
    elif gran_str == 'Monthly':
        return timestamp + dateutil.relativedelta.relativedelta(months=offset)
    elif gran_str == 'Yearly':
        return timestamp + dateutil.relativedelta.relativedelta(years=offset)
    elif gran_str == 'Hourly':
        return timestamp + datetime.timedelta(hours=offset)
    elif gran_str == 'Minutely':
        return timestamp + datetime.timedelta(minutes=offset)
    elif gran_str == 'Secondly':
        return timestamp + datetime.timedelta(seconds=offset)
    elif gran_str == 'Custom':
        return timestamp + datetime.timedelta(seconds=custom_in_seconds * offset)
    else:
        raise Exception('Granularity not supported: {}|{}'.format(*graninfo))

def get_time_list(start_time, end_time, graninfo):
    time_list = []
    
    (gran_str, custom_in_seconds) = graninfo
    offset = 1
    if gran_str == 'Daily':
        timedelta = datetime.timedelta(days=offset)
    elif gran_str == 'Weekly':
        timedelta = datetime.timedelta(weeks=offset)
    elif gran_str == 'Monthly':
        timedelta = dateutil.relativedelta.relativedelta(months=offset)
    elif gran_str == 'Yearly':
        timedelta = dateutil.relativedelta.relativedelta(years=offset)
    elif gran_str == 'Hourly':
        timedelta = datetime.timedelta(hours=offset)
    elif gran_str == 'Minutely':
        timedelta = datetime.timedelta(minutes=offset)
    elif gran_str == 'Secondly':
        timedelta = datetime.timedelta(seconds=offset)
    elif gran_str == 'Custom':
        timedelta = datetime.timedelta(seconds=custom_in_seconds * offset)
    else:
        raise Exception('Granularity not supported: {}|{}'.format(*graninfo))

    while start_time <= end_time:
        time_list.append(start_time)
        start_time = start_time + timedelta

    return time_list