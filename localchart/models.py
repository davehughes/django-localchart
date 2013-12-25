from django.db import models
from django.db.models.query import QuerySet
from model_utils.managers import PassThroughManager


class DailyMetricQuerySet(QuerySet):

    def list_metrics(self):
        combos = self.values_list('source', 'metric').order_by().distinct()
        return ['{}:{}'.format(s, m) for s, m in combos]

    def for_metric(self, metric):
        '''
        Actually, for a source-metric combination as would be listed by
        list_metrics().
        '''
        source, _, metric = metric.partition(':')
        return self.filter(source=source, metric=metric)


class DailyMetric(models.Model):
    source = models.CharField(max_length=50)
    metric = models.CharField(max_length=50)
    date = models.DateField()
    value = models.DecimalField(decimal_places=2, max_digits=20)

    objects = PassThroughManager.for_queryset_class(DailyMetricQuerySet)()

    class Meta:
        unique_together = ('source', 'metric', 'date')
        ordering = ('date',)
