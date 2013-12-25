Timeframes
==========

Localchart builds its results based 'timeframes' that can be created explicitly
or derived from a relative string like 'today':

```python
from localchart import timeframes

explicit_timeframe = {
    'start': datetime.datetime(2013, 1, 1),
    'end': datetime.datetime(2014, 1, 1),
    }

derived_timeframe = 'this_year'

resolve_timeframe(explicit_timeframe)
>>> {'end': datetime.datetime(2014, 1, 1, 0, 0, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
>>>  'start': datetime.datetime(2013, 1, 1, 0, 0, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>)}

resolve_timeframe(derived_timeframe)
>>> {'end': datetime.datetime(2013, 12, 25, 15, 32, 46, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
>>>  'start': datetime.datetime(2012, 12, 25, 15, 32, 46, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>)} 

# Timeframes can also yield a 'quantized' resolution, where the given period is
# rounded down to its beginning.
resolve_timeframe(derived_timeframe, quantize=True)
>>> {'end': datetime.datetime(2014, 1, 1, 0, 0, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
>>>  'start': datetime.datetime(2013, 1, 1, 0, 0, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>}
```

The timeframes available are:
+ 'today'
+ 'yesterday'
+ 'this_{period}'
+ 'this_{n}_{period}s'
+ '(last|previous)_{period}'
+ '(last|previous)_{n}_{period}s'
