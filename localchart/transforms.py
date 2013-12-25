import math
from itertools import chain, izip


def absolute_growth(series):
    '''
    absolute_growth([10, 12, 12, 9, 15]) -> [None, 2, 0, -3, 6]
    '''
    growth = lambda n1, n2: n2 - n1
    return chain([None],
                 (growth(n1, n2) for n1, n2 in izip(series[:-1], series[1:])))


def percentage_growth(series):
    '''
    percentage_growth([10, 12, 12, 9, 15]) -> [None, 20.00, 0.00, -25, 66.66]
    '''
    growth = lambda n1, n2: (((n2 - n1) * 100.0) / n1) if n1 else float('inf')
    return chain([None],
                (growth(float(n1), float(n2))
                 for n1, n2 in izip(series[:-1], series[1:])))


def relative_to(series, normal=0):
    '''
    relative_to([1, 2, 3, 4, 5], 3) -> [-2, -1, 0, 1, 2]
    '''
    return (s - normal for s in series)


def cumulative_count(series, baseline=0):
    '''
    cumulative_count([1, 2, 3, 2, 1])              -> [ 1,  3,  6,  8,  9]
    cumulative_count([1, 2, 3, 2, 1], baseline=10) -> [11, 13, 16, 18, 19]
    '''
    cc = baseline
    for s in series:
        yield s + cc
        cc += s


def replace_nulls(series, replace_with=0):
    return [s if s else replace_with for s in series]


def avg(series):
    return [sum(series) / len(series)]


def stddev(series):
    return [math.sqrt(variance(series)[0])]


def variance(series):
    series_avg = avg(series)[0]
    return [sum([pow((x / (series_avg * 1.00)), 2) for x in series])]


TRANSFORM_FUNCTIONS = {
    'growth': absolute_growth,
    'growth_pct': percentage_growth,
    'cumulative': cumulative_count,
    'avg': avg,
    'stddev': stddev,
    'variance': variance,
}
