import datetime
import json

from django import http
from django.contrib.auth.decorators import user_passes_test
from django.db.models import F, Q, Count, Sum, Avg, Min, Max, StdDev, Variance
from django.views.decorators.http import require_http_methods

from localchart import common, timeframes


@require_http_methods(['GET'])
@user_passes_test(lambda user: user.is_staff)
def query(request):
    metric = request.REQUEST.get('metric')
    run_report = common.get_report_runner(metric)
    if not run_report:
        raise http.Http404()

    report = run_report(**request.GET.dict())
    return http.HttpResponse(json.dumps(report, cls=common.JSONEncoder),
                             content_type='application/json')


