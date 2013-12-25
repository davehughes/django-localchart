import calendar
import datetime
import itertools
import re
import types

from django.utils import timezone
from pytz import AmbiguousTimeError


def resolve_timeframe(timeframe, quantize=False):
    if type(timeframe) in types.StringTypes:
        return get_timeframe_from_text(timeframe, quantize=quantize)

    if 'start' in timeframe and 'end' in timeframe:
        timeframe['start'] = resolve_date(timeframe['start'])
        timeframe['end'] = resolve_date(timeframe['end'])

    return timeframe


def resolve_literal_timeframe(start_date, end_date, formats=None, tz=None):
    return {
        'start': resolve_date(start_date, formats=formats, tz=tz),
        'end': resolve_date(end_date, formats=formats, tz=tz),
        }


def resolve_date(d, formats=None, tz=None):

    if not d:
        return None

    if type(d) in [datetime.date, datetime.datetime]:
        return coerce_to_aware_datetime(d, tz=tz)

    if type(d) in types.StringTypes:
        formats = formats or ['%Y-%m-%d']
        for f in formats:
            try:
                d = datetime.datetime.strptime(d, f)
                return coerce_to_aware_datetime(d, tz=tz)
            except:
                pass

    raise Exception('Unresolvable date: {}'.format(d))


def coerce_to_aware_datetime(dt, tz=None):
    tz = tz or timezone.get_current_timezone()
    if type(dt) is datetime.date:
        dt = datetime.datetime.combine(dt, datetime.time())
    if not timezone.is_aware(dt):
        try:
            dt = timezone.make_aware(dt, tz)
        except AmbiguousTimeError as e:
            dt = tz.localize(dt)
    return dt


def get_timeframe_from_text(text, relative_to=None, quantize=False):
    '''
    Implements keen.io-style relative timeframes.
    
    Supported units:
        'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'

    Usage: (assuming relative_to=datetime.datetime(2013, 7, 30, 21, 12, 26, 960879))

        get_timeframe_from_text('last_week')
            -> {'start': datetime.datetime(2013, 7, 22, 0, 0),
                'end':   datetime.datetime(2013, 7, 29, 0, 0)},

        get_timeframe_from_text('this_week')
            -> {'start': datetime.datetime(2013, 7, 29, 0, 0),
                'end':   datetime.datetime(2013, 8, 5, 0, 0)}

        get_timeframe_from_text('yesterday')
        get_timeframe_from_text('previous_day')
        get_timeframe_from_text('previous_1_days')
            -> {'start': datetime.datetime(2013, 7, 29, 0, 0),
                'end':   datetime.datetime(2013, 7, 30, 0, 0)}

        get_timeframe_from_text('this_month')
            -> {'start': datetime.datetime(2013, 7, 1, 0, 0),
                'end':   datetime.datetime(2013, 8, 1, 0, 0)}

        get_timeframe_from_text('previous_2_quarters')
            -> {'start': datetime.datetime(2013, 1, 1, 0, 0),
                'end':   datetime.datetime(2013, 7, 1, 0, 0)}

        get_timeframe_from_text('this_year')
            -> {'start': datetime.datetime(2013, 1, 1, 0, 0),
                'end':   datetime.datetime(2014, 1, 1, 0, 0)}
    '''
    aliases = {
        'yesterday': 'previous_1_days',
        'today': 'this_1_days',
        # 'tomorrow': 'next_1_days',
        }
    from_offsets = {
        'this': 0,
        # 'next': 1,
        'previous': -1,
        'last': -1,
        }
    orig_text = text
    text = aliases.get(text, text)
    
    matched = False
    from_unit = re.compile(r'^(?P<from>this|next|previous|last)_(?P<unit>\w+)$')
    from_units = re.compile(r'^(?P<from>this|next|previous|last)_(?P<span>\d+)_(?P<unit>\w+)s$')

    m = from_units.match(text)
    if m:
        from_, span, unit = m.groups()
        span = int(span)
        offset = from_offsets[from_]
        matched = True

    if not matched:
        m = from_unit.match(text)
        if m:
            from_, unit = m.groups()
            offset = from_offsets[from_]
            span = 1
            matched = True

    if not matched:
        raise Exception('Unrecognized timeframe format: {}'.format(orig_text))

    return get_relative_timeframe(unit, offset=offset, span=span,
                                  relative_to=relative_to, quantize=quantize)


def get_relative_timeframe(unit, offset=0, span=1, quantize=False, relative_to=None):
    relative_to = coerce_to_aware_datetime(relative_to or timezone.now())

    units = QUANTIZED_TIMEFRAME_UNITS if quantize else TIMEFRAME_UNITS
    timeframe = units.get(unit)
    if not timeframe:
        raise Exception('Unrecognized timeframe unit: {}'.format(unit))

    # When using quantized periods, expand the period by 1 frame to
    # contain the current datetime
    if quantize:
        offset += 1

    return {
        'start': timeframe.floor(relative_to, offset=offset-span),
        'end': timeframe.floor(relative_to, offset=offset),
        }


def get_timeframe_divisions(timeframe, unit, quantize=False):
    '''
    Given a timeframe and a unit to divide by, return a list of timeframes
    representing normalized subdivisions of `timeframe` by the `unit`.
    '''
    if not timeframe and not unit:
        return []

    timeframe = resolve_timeframe(timeframe, quantize=quantize)

    # If no division units are specified, return the full timeframe
    if not unit:
        return [timeframe]

    units = QUANTIZED_TIMEFRAME_UNITS if quantize else TIMEFRAME_UNITS
    division_timeframe = units.get(unit)
    if not division_timeframe:
        raise Exception('Unrecognized timeframe unit: {}'.format(unit))

    return division_timeframe.time_periods(timeframe['start'], timeframe['end'])


class Timeframe(object):

    def ceil(self, dt, offset=0):
        return self.floor(dt, offset=offset+1)

    def this_period(self, dt, offset=0):
        return (self.floor(dt, offset=offset), self.ceil(dt, offset=offset))

    def previous_period(self, dt):
        return self.this_period(dt, offset=-1)

    def next_period(self, dt):
        return self.this_period(dt, offset=1)

    def time_periods(self, start, end):
        endpts = (self.floor(end, offset=-x) for x in itertools.count())
        start_dates, end_dates = itertools.tee(endpts)
        start_dates.next()  # drop first
        periods = itertools.takewhile(lambda (s, e): e > start, itertools.izip(start_dates, end_dates))
        return [{'start': s, 'end': e} for s, e in reversed(list(periods))]


class QuantizedTimeframe(object):

    def time_periods(self, start, end):
        endpts = (self.floor(end, offset=-x+1) for x in itertools.count())
        start_dates, end_dates = itertools.tee(endpts)
        start_dates.next()  # drop first
        periods = itertools.takewhile(lambda (s, e): e >= start, itertools.izip(start_dates, end_dates))
        return [{'start': s, 'end': e} for s, e in reversed(list(periods))]


class MinuteTimeframe(Timeframe):
    unit = 'minute'

    def floor(self, dt, offset=0):
        floor = dt + datetime.timedelta(minutes=offset)
        return coerce_to_aware_datetime(floor)


class HourTimeframe(Timeframe):
    unit = 'hour'

    def floor(self, dt, offset=0):
        floor = dt + datetime.timedelta(hours=offset)
        return coerce_to_aware_datetime(floor)


class DayTimeframe(Timeframe):
    unit = 'day'

    def floor(self, dt, offset=0):
        floor = dt + datetime.timedelta(days=offset)
        return coerce_to_aware_datetime(floor)


class WeekTimeframe(Timeframe):
    unit = 'week'

    def floor(self, dt, offset=0):
        floor = dt + datetime.timedelta(days=offset * 7)
        return coerce_to_aware_datetime(floor)


class MonthTimeframe(Timeframe):
    unit = 'month'

    def floor(self, dt, offset=0):
        qfloor = QuantizedMonthTimeframe().floor(dt, offset=offset)
        _, days_in_floor_month = calendar.monthrange(qfloor.year, qfloor.month)
        floor_day = min(dt.day, days_in_floor_month)
        floor = datetime.datetime(qfloor.year, qfloor.month, floor_day,
                                  dt.hour, dt.minute, dt.second)
        return coerce_to_aware_datetime(floor)


class YearTimeframe(Timeframe):
    unit = 'year'

    def floor(self, dt, offset=0):
        floor_year = (dt.year + offset)
        _, days_in_floor_month = calendar.monthrange(floor_year, dt.month)
        floor_day = min(dt.day, days_in_floor_month)
        floor = datetime.datetime(floor_year, dt.month, floor_day,
                                  dt.hour, dt.minute, dt.second)
        return coerce_to_aware_datetime(floor)


class QuarterTimeframe(Timeframe):
    unit = 'quarter'

    def floor(self, dt, offset=0):
        return  MonthTimeframe().floor(dt, offset=(offset*3))


class QuantizedMinuteTimeframe(QuantizedTimeframe):
    unit = 'minute'

    def floor(self, dt, offset=0):
        floor = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)
        floor = coerce_to_aware_datetime(floor)
        return floor + datetime.timedelta(minutes=offset)


class QuantizedHourTimeframe(QuantizedTimeframe):
    unit = 'hour'

    def floor(self, dt, offset=0):
        floor = datetime.datetime(dt.year, dt.month, dt.day, dt.hour)
        floor = coerce_to_aware_datetime(floor)
        return floor + datetime.timedelta(hours=offset)


class QuantizedDayTimeframe(QuantizedTimeframe):
    unit = 'day'

    def floor(self, dt, offset=0):
        floor = datetime.datetime(dt.year, dt.month, dt.day)
        floor = coerce_to_aware_datetime(floor)
        return floor + datetime.timedelta(days=offset)


class QuantizedWeekStartDays(object):
    Monday = 0
    Tuesday = 1
    Wednesday = 2
    Thursday = 3
    Friday = 4
    Saturday = 5
    Sunday = 6

QUANTIZED_WEEK_START_DAY=QuantizedWeekStartDays.Sunday


class QuantizedWeekTimeframe(QuantizedTimeframe):
    unit = 'week'

    def floor(self, dt, offset=0, start_of_week_day=None):
        start_of_week_day = start_of_week_day or QUANTIZED_WEEK_START_DAY
        days_offset = (dt.weekday() - start_of_week_day) % 7
        floor = QuantizedDayTimeframe().floor(dt - datetime.timedelta(days=days_offset))
        floor = coerce_to_aware_datetime(floor)
        return floor + datetime.timedelta(weeks=offset)


class QuantizedMonthTimeframe(QuantizedTimeframe):
    unit = 'month'
    
    def floor(self, dt, offset=0):
        floor = datetime.datetime(dt.year, dt.month, 1)
        rel_months = floor.month + offset
        month = ((rel_months - 1) % 12) + 1
        years_carry = (rel_months - 1) / 12
        year = floor.year + (years_carry)
        return coerce_to_aware_datetime(datetime.datetime(year, month, 1))


class QuantizedYearTimeframe(QuantizedTimeframe):
    unit = 'year'

    def floor(self, dt, offset=0):
        floor = datetime.datetime(dt.year + offset, 1, 1)
        floor = coerce_to_aware_datetime(floor)
        return floor

class QuantizedQuarterTimeframe(QuantizedTimeframe):
    unit = 'quarter'

    def floor(self, dt, offset=0):
        offset_month = QuantizedMonthTimeframe().floor(dt, offset=offset*3)
        qmonth = ((((offset_month.month - 1) / 3) * 3) + 1) % 12
        floor = datetime.datetime(offset_month.year, qmonth, offset_month.day)
        floor = coerce_to_aware_datetime(floor)
        return floor


TIMEFRAME_UNITS = {
    'minute': MinuteTimeframe(),
    'hour': HourTimeframe(),
    'day': DayTimeframe(),
    'week': WeekTimeframe(),
    'month': MonthTimeframe(),
    'year': YearTimeframe(),
    'quarter': QuarterTimeframe(),
}
QUANTIZED_TIMEFRAME_UNITS = {
    'minute': QuantizedMinuteTimeframe(),
    'hour': QuantizedHourTimeframe(),
    'day': QuantizedDayTimeframe(),
    'week': QuantizedWeekTimeframe(),
    'month': QuantizedMonthTimeframe(),
    'year': QuantizedYearTimeframe(),
    'quarter': QuantizedQuarterTimeframe(),
}
