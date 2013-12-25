import datetime
import decimal
import json
import types

from django.db.models import F, Q, Count, Sum, Avg, Min, Max, StdDev, Variance
from django.db.models.related import RelatedObject
from django.db.models.fields.related import RelatedField
from django.utils.encoding import force_unicode

from localchart import timeframes, transforms


def query(metric=None, **kwargs):
    run_report = get_report_runner(metric)
    if not run_report:
        raise Exception('Unrecognized metric: {}'.format(metric))
    return run_report(**kwargs)


def run_timeframe_query(qs,
                        timeframe=None,
                        start_date=None,
                        end_date=None,
                        time_divisions=None,
                        timestamp_relation=None,
                        timeframe_limiter='standard',
                        aggregate_type='count',
                        aggregate_field='pk',
                        group_by=None,
                        transforms=None,
                        quantize=False,
                        **kwargs):

    aggregate_by = resolve_aggregation(aggregate_type, aggregate_field)
    transforms = resolve_transforms(transforms)

    if timeframe and (start_date or end_date):
        raise Exception('Invalid timeframe arguments: either pass a start and '
                        'end date or a recognized timeframe string')

    if start_date or end_date:
        timeframe = timeframes.resolve_literal_timeframe(start_date, end_date)
    elif timeframe:
        quantize_timeframe = quantize if not time_divisions else False
        timeframe = timeframes.resolve_timeframe(timeframe, quantize=quantize_timeframe)
    elif timestamp_relation:
        # Calculate the full time range of the data and use it as the timeframe
        timeframe_bounds = qs.aggregate(start=Min(timestamp_relation), end=Max(timestamp_relation))
        timeframe = {'start': timeframe_bounds['start'],
                     'end': timeframe_bounds['end'] + datetime.timedelta(seconds=1)}

    # Get the specified timeframe limiting function, or a non-limiting function
    # if none is provided
    if timeframe:
        limit_timeframe = timeframes.resolve_timeframe_limiter(timeframe_limiter)
    else:
        limit_timeframe = no_timeframe_limiter

    # If grouping, identify reverse relation properties for later use
    if group_by:
        group_by_model, group_by_reverse_rel, group_by_prop = get_reverse_rel_path(qs.model, group_by)
        if group_by_reverse_rel:
            aggregate_by.lookup = '{}__{}'.format(group_by_reverse_rel, aggregate_by.lookup)
    else:
        group_by_model, group_by_reverse_rel, group_by_prop = None, None, None

    # Do a top-level query to enumerate all series for the full timeframe (only needed for group_by)
    if group_by:
        full_qs = limit_timeframe(qs, timeframe, timestamp_relation)
        if group_by_reverse_rel:
            full_group_by_qs = group_by_model.objects.filter(**{'{}__in'.format(group_by_reverse_rel): full_qs}).order_by()
        else:
            full_group_by_qs = full_qs

        def row_to_label(row):
            if group_by_prop:
                return get_field_display(qsd.model, field_name=group_by_prop, value=row[group_by_prop])
            else:
                return force_unicode(row, strings_only=True)

        if group_by_prop:
            full_group_by_qs = full_group_by_qs.values(group_by_prop).distinct().order_by()
            series = [{'id': r[group_by_prop], 'name': get_field_display(full_group_by_qs.model, field_name=group_by_prop, value=r[group_by_prop])}
                      for r in full_group_by_qs]
        else:
            full_group_by_qs = full_group_by_qs.distinct().order_by()
            series = [{'id': r.pk, 'name': unicode(r)} for r in full_group_by_qs.distinct()]
    else:
        agg_str = aggregation_to_string(aggregate_by)
        series = [{'id': agg_str, 'name': agg_str}]

    def result_for_time_division(division):

        # Put together a primary query bounded by the timeframe
        qsd = limit_timeframe(qs, division, timestamp_relation) if division else qs

        if group_by:

            # If a reverse relation exists, we switch our primary queryset to
            # the set of objects that has that reverse relationship to the
            # objects in the previous queryset.
            if group_by_reverse_rel:
                group_qsd = group_by_model.objects.filter(**{'{}__in'.format(group_by_reverse_rel): qsd}).order_by()
            else:
                group_qsd = qsd

            group_qsd = group_qsd.values(group_by_prop or 'pk')

            group_qsd = group_qsd.annotate(value=aggregate_by)
            values = {row[group_by_prop or 'pk']: row['value']
                      for row in group_qsd.annotate(value=aggregate_by)}
            return (division, values)
        else:
            aggregated_value = qsd.aggregate(value=aggregate_by)['value']
            aggregation_name = aggregation_to_string(aggregate_by)
            return (division, {aggregation_name: aggregated_value})

    # Next, break the full timeframe into divisions and query for each division
    divisions = timeframes.get_timeframe_divisions(timeframe, time_divisions, quantize=quantize)
    timeframe = {'start': divisions[0]['start'], 'end': divisions[-1]['end']}
    results = [result_for_time_division(d) for d in divisions or [None]]

    # Collate series results
    for division, r in results:
        for s in series:
            s.setdefault('data', []).append(r.get(s['id']))

    # Apply any transforms
    for transform in transforms or []:
        t = transforms.TRANSFORM_FUNCTIONS.get(transform)
        if not t:
            continue
        for s in series:
            s['data'] = list(t(s['data']))

    # TODO: add metadata based on a values() query

    # Build result, including metadata about the query
    result = {
        'query': {},
        'series': series,
        }

    if timeframe:
        result['query'].setdefault('timeframe', {}).update({
                'start': timeframe['start'],
                'end': timeframe['end'],
                })
    else:
        result['query'].setdefault('timeframe', {}).update({'start': None, 'end': 'None'})

    if aggregate_by:
        result['query']['aggregate_type'] = aggregate_type
        result['query']['aggregate_field'] = aggregate_field

    if transforms:
        result['query']['transforms'] = transforms

    if time_divisions:
        result['query'].setdefault('timeframe', {})['periods'] = [
            {'start': div['start'], 'end': div['end']} for div in divisions
            ]
        result['query']['time_divisions'] = time_divisions

    if group_by:
        result['query']['group_by'] = group_by

    result['query']['datashape'] = sniff_data_shape(result)

    return result


def sniff_data_shape(data):
    series = data.get('series')
    if not series:
        raise Exception('No series found in data frame')
    elif len(series) == 0:
        return 'empty'
    elif len(series) == 1:
        subvalues = series[0].get('data')
        if not subvalues:
            raise Exception('No values found in data subframe')
        elif len(subvalues) == 0:
            return 'empty'
        elif len(subvalues) == 1:
            return 'scalar'
        else:
            return 'single-series'
    else:
        subvalues = series[0].get('data')
        if not subvalues:
            raise Exception('No values found in data subframe')
        elif len(subvalues) == 0:
            return 'empty'
        elif len(subvalues) == 1:
            return 'single-frame'
        else:
            return 'multi-series'


def get_reverse_rel_path(source_model, forward_rel_path):
    '''
    Given a double-underscore separated path representing a forward relation,
    return a tuple of:
      - Target model
      - Path representing the reverse relation from the target to the 
        source.

    Example:
      A Post has multiple Comments, each of which has a User that posted it.
      Assuming no custom related_names on the foreign keys:

      get_reverse_rel_path(User, 'comment__post') -> (Post, 'comment__user')
      get_reverse_rel_path(User, 'comment') -> (Comment, 'user')
    '''
    rels = forward_rel_path.split('__')
    model = source_model
    reverse_rel_path = ''
    rel_property = None

    for idx, rel in enumerate(rels):
        related = model._meta.get_field_by_name(rel)[0]
        if isinstance(related, RelatedObject):
            reverse_rel_name = related.field.name
            model = related.field.model
        elif isinstance(related, RelatedField):
            reverse_rel_name = related.related_query_name()
            model = related.rel.to
        else:
            if idx < len(rels) - 1:
                raise Exception('Non-relational property found as an intermediate relation')
            rel_property = rel
            continue
        
        if reverse_rel_path:
            reverse_rel_path = '{}__{}'.format(reverse_rel_name, reverse_rel_path)
        else:
            reverse_rel_path = reverse_rel_name

    return model, reverse_rel_path, rel_property


def get_field_display(model, field_name=None, value='(none)'):
    '''
    Performs the same conversion as get_FIELD_display() in Django's base model
    without requiring a model instance.

    Usage:
        class Person(models.Model):
            ...
            sex = fields.CharField(choices=(('f', 'Female'), ('m', 'Male')))

        get_field_display(Person, 'sex', 'f') -> 'Female'
    '''
    field, _, _, _ = model._meta.get_field_by_name(field_name)
    return force_unicode(dict(field.flatchoices).get(value, value), strings_only=True)


def aggregation_to_string(aggregation):
    '''
    Converts an aggregation function to a string describing it.

    Usage:
        aggregation_to_string(Count('first_name')) -> 'count_first_name'
        aggregation_to_string(Avg('value'))        -> 'avg_value'
    '''
    return '{}_{}'.format(aggregation.name, aggregation.lookup).lower()


def no_timeframe_limiter(qs, timeframe, timestamp_relation):
    return qs


def standard_timeframe_limiter(qs, timeframe, timestamp_relation):
    return qs.filter(**{'{}__gte'.format(timestamp_relation): timeframe['start'],
                        '{}__lt'.format(timestamp_relation): timeframe['end']})


def cumulative_timeframe_limiter(qs, timeframe, timestamp_relation):
    return qs.filter(**{'{}__lt'.format(timestamp_relation): timeframe['end']})


TIMEFRAME_LIMITERS = {
    'none': no_timeframe_limiter,
    'standard': standard_timeframe_limiter,
    'cumulative': cumulative_timeframe_limiter,
}


def resolve_timeframe_limiter(limiter):
    if type(limiter) is types.FunctionType:
        return limiter
    else:
        return TIMEFRAME_LIMITERS[limiter]


AGGREGATION_FUNCTIONS = {
    'sum': Sum,
    'avg': Avg,
    'count': Count,
    'min': Min,
    'max': Max,
    'stddev': StdDev,
    'variance': Variance,
    }


def resolve_aggregation(type, field):
    func = AGGREGATION_FUNCTIONS.get(type)
    if not func:
        raise Exception('Unrecognized aggregation function: {}'.format(type))
    return func(field)


def resolve_transforms(transforms):
    if not transforms:
        return []
    if type(transforms) in types.StringTypes:
        return transforms.split('|')
    return transforms


REPORT_REGISTRY = {}

def report(f):
    REPORT_REGISTRY[f.func_name] = f
    return f


def get_report_runner(report_name):
    from localchart import models
    report_runner = REPORT_REGISTRY.get(report_name)
    if not report_runner:
        if models.DailyMetric.objects.for_metric(report_name).exists():
            return daily_metric_view(report_name)
    return report_runner


def daily_metric_view(metric, name=None):
    def run_report(**kwargs):
        from localchart import models
        kwargs.setdefault('timestamp_relation', 'date')
        kwargs.setdefault('aggregate_type', 'sum')
        kwargs.setdefault('aggregate_field', 'value')
        return run_timeframe_query(
            models.DailyMetric.objects.for_metric(metric),
            **kwargs
            )
    run_report.func_name = str(name or metric)
    return report(run_report)


class JSONEncoder(json.JSONEncoder):
    """
    Slightly more intelligent JSON encoder class, based on the one from
    Django REST Framework. Handles commonly used types like decimals and
    dates/times in a sane way.
    """
    def default(self, o):
        # For Date Time string spec, see ECMA 262
        # http://ecma-international.org/ecma-262/5.1/#sec-15.9.1.15
        if isinstance(o, datetime.datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith('+00:00'):
                r = r[:-6] + 'Z'
            return r
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, datetime.time):
            if timezone and timezone.is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, datetime.timedelta):
            return str(o.total_seconds())
        elif isinstance(o, decimal.Decimal):
            return str(o)
        elif hasattr(o, '__iter__'):
            return [i for i in o]
        return super(JSONEncoder, self).default(o)
