from django.conf.urls import patterns, include, url


urlpatterns = patterns('',
        url(r'^query/$', 'localchart.views.query', name='localchart-query'),
)
