# -*- encoding: utf-8 -*-
#
# Copyright © 2016 The University of Melbourne
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import daiquiri
import datetime
try:
    import queue
except ImportError:
    # python2
    import Queue as queue
from gnocchi import incoming
from gnocchi import utils
from gnocchi.storage.common import influxdb as influxdb_common


LOG = daiquiri.getLogger(__name__)


class InfluxDBStorage(incoming.IncomingDriver):

    # Needed to mimic cabonara driver
    NUM_SACKS = 0

    def __init__(self, conf, greedy=True):
        super(InfluxDBStorage, self).__init__(conf, greedy)
        self.influx = influxdb_common.get_connection(conf)
        self.database = conf.influxdb_database
        self.spool_size = conf.influxdb_batch_size
        self.spool_queues = {}

    @staticmethod
    def upgrade(sacks=None):
        pass

    def _get_queue(self, retention_policy):
        if retention_policy not in self.spool_queues:
            self.spool_queues[retention_policy] = queue.Queue(
                self.spool_size * 10)
            LOG.debug("Created spool queue for %s", retention_policy)
        return self.spool_queues[retention_policy]

    def _batch_points(self, retention_policy, points):
        overflow = False
        queue = self._get_queue(retention_policy)
        for point in points:
            self._maybe_flush(retention_policy, queue)
            try:
                queue.put_nowait(point)
            except queue.Full:
                overflow = True
            if overflow:
                LOG.warning('Failed to record metering data: '
                            'meter spool is full.')

    def _maybe_flush(self, retention_policy, queue):
        LOG.debug('Queue size %s/%s', queue.qsize(), self.spool_size)
        if queue.qsize() >= self.spool_size:
            self._flush(retention_policy, queue)

    def _flush(self, retention_policy, queue):
        points = []
        while len(points) < self.spool_size:
            try:
                points.append(queue.get_nowait())
            except queue.Empty:
                break
        LOG.debug("Sending %s points", len(points))
        self.influx.write_points(points=points,
                                 time_precision='n',
                                 database=self.database,
                                 retention_policy=retention_policy)

    def _get_incoming_measurement(self, ap):
        return "%s_%s" % (influxdb_common.MEASUREMENT_PREFIX,
                          ap.replace('-', ''))

    def _get_incoming_rp_name(self, ap):
        return "rp_%s_incoming" % ap.replace('-', '')

    def add_measures(self, metric, measures):
        """Add a measure to a metric.

        :param metric: The metric measured.
        :param measures: The actual measures.
        """
        self.add_measures_batch({metric: measures})

    def add_measures_batch(self, metrics_and_measures):
        """Add a batch of measures for some metrics.

        :param metrics_and_measures: A dict where keys
        are metrics and value are measure.
        """
        for metric, measures in metrics_and_measures.items():
            try:
                ap_name = metric.archive_policy.name
            except Exception:
                ap_name = metric.archive_policy_name
            measurement = self._get_incoming_measurement(ap_name)
            points = [dict(measurement=measurement,
                           time=m.timestamp.astype(datetime.datetime),
                           fields=dict(value=float(m.value)),
                           tags=dict(metric_id=str(metric.id)))
                      for m in measures]
            rp = self._get_incoming_rp_name(ap_name)
            if self.spool_size > 0:
                self._batch_points(rp, points)
            else:
                self.influx.write_points(points=points,
                                         time_precision='n',
                                         database=self.database,
                                         retention_policy=rp)

    def measures_report(self, details=True):
        """Return a report of pending to process measures.

        Only useful for drivers that process measurements in background

        :return: {'summary': {'metrics': count, 'measures': count},
                  'details': {metric_id: pending_measures_count}}
        """
        report = {'summary': {'metrics': 0, 'measures': 0}}
        if details:
            report['details'] = {}
        return report
