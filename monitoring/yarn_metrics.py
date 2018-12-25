"""Handling metrics."""
import datetime
import logging

import backoff
import googleapiclient.discovery
from google.auth import app_engine
from googleapiclient.errors import HttpError
from  metrics import get_start_time, get_now_rfc3339
from util import utils


SCOPES = ('https://www.googleapis.com/auth/monitoring',
          'https://www.googleapis.com/auth/cloud-platform')
CREDENTIALS = app_engine.Credentials(scopes=SCOPES)


class YarnMetrics(object):
    """Writing and reading metrics."""

    def __init__(self, cluster_name):
        self.monitorservice = googleapiclient.discovery. \
            build('monitoring',
                  'v3',
                  credentials=CREDENTIALS)
        self.project_id = utils.get_project_id()
        self.project_resource = "projects/{0}".format(self.project_id)
        self.metric_domain = 'dataproc.googleapis.com'
        self.cluster_name = cluster_name


    def read_timeseries(self, custom_metric_type):
        """
        Get the time series from stackdriver.

        :param custom_metric_type:
        :param minutes:
        :return: json object
        """

        out = []
        custom_metric = '{}/{}'.format(self.metric_domain, custom_metric_type)
        default_request_kwargs = dict(
            name=self.project_resource,
            filter='metric.type="{0}" AND resource.labels.cluster_name="{1}"'.
            format(custom_metric, self.cluster_name),

            interval_startTime=get_start_time(1),
            interval_endTime=get_now_rfc3339())


        @backoff.on_exception(
            backoff.expo, HttpError, max_tries=3, giveup=utils.fatal_code)
        def _do_request(next_page_token=None):
            kwargs = default_request_kwargs.copy()
            if next_page_token:
                kwargs['nextPageToken'] = next_page_token
            req = self.monitorservice.projects().timeSeries().list(**kwargs)
            return req.execute()

        try:
            response = _do_request()

            out.extend(response.get('timeSeries', []))

            next_token = response.get('nextPageToken')

            while next_token:
                response = _do_request(next_token)
                out.extend(response.get('timeSeries', []))
                next_token = response.get('nextPageToken')
        except HttpError as e:
            logging.info(e)
        return out

    def get_memory_metrics(self):
        res = self.read_timeseries('cluster/yarn/memory_size')
        mem ={}
        mem['available'] = res[0]['points'][0]['value']['doubleValue']*1024
        mem['allocated'] = res[1]['points'][0]['value']['doubleValue']*1024
        mem['reserved'] =  res[2]['points'][0]['value']['doubleValue']*1024
        return mem

    def get_containers_metrics(self):
        res = self.read_timeseries('cluster/yarn/containers')
        containers ={}
        containers['allocated'] = res[0]['points'][0]['value']['int64Value']
        containers['pending'] = res[1]['points'][0]['value']['int64Value']
        containers['reserved'] =  res[2]['points'][0]['value']['int64Value']
        return containers

    def get_nodes_metrics(self):
        res = self.read_timeseries('cluster/yarn/nodemanagers')
        nodes ={}
        nodes['active'] = res[0]['points'][0]['value']['int64Value']
        nodes['decommissioned'] = res[1]['points'][0]['value']['int64Value']
        nodes['lost'] =  res[2]['points'][0]['value']['int64Value']
        nodes['rebooted'] = res[3]['points'][0]['value']['int64Value']
        nodes['unhealthy'] = res[4]['points'][0]['value']['int64Value']
        return nodes
