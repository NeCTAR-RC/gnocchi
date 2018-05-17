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
from __future__ import absolute_import
import daiquiri
import operator
import re
import numpy

try:
    import influxdb
except ImportError:
    influxdb = None
import iso8601
from oslo_config import cfg
from oslo_utils import timeutils

from gnocchi import carbonara
from gnocchi import indexer
from gnocchi import storage
from gnocchi.common import influxdb as influxdb_common


OPTS = [
    cfg.StrOpt('influxdb_host',
               default='localhost',
               help='InfluxDB host'),
    cfg.IntOpt('influxdb_port',
               default=8086,
               help='InfluxDB port'),
    cfg.StrOpt('influxdb_username',
               default='root',
               help='InfluxDB username'),
    cfg.StrOpt('influxdb_password',
               secret=True,
               help='InfluxDB password'),
    cfg.StrOpt('influxdb_database',
               default='gnocchi',
               help='InfluxDB database'),
    cfg.BoolOpt('influxdb_disable_retention_policies',
                default=False,
                help='InfluxDB disable retention policies, '
                'essentially setting all retention policies to '
                'infinity'),
    cfg.IntOpt('influxdb_batch_size',
               default=0,
               help='InfluxDB batch size to spool points'),
]


LOG = daiquiri.getLogger(__name__)


class InfluxDBStorage(storage.StorageDriver):

    CQ_QUERY = """CREATE CONTINUOUS QUERY %(measure)s ON "%(database)s" BEGIN
    SELECT %(aggregation_method)s AS value INTO
    "%(database)s"."%(retention)s".%(measure)s FROM
    "%(database)s"."%(parent_retention)s".%(parent_measure)s GROUP BY
    time(%(granularity)ss), metric_id END"""

    def __init__(self, conf, coord=None):
        super(InfluxDBStorage, self).__init__(conf, coord)
        self.influx = influxdb_common.get_connection(conf)
        self.database = conf.influxdb_database
        self.influxdb_disable_retention_policies = \
            conf.influxdb_disable_retention_policies
        self.conf = conf

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.database))

    def setup_archive_policy(self, ap, reset=False):
        incoming_measure = self._get_incoming_measurement(ap)
        # Set up an archive policy for incoming measures
        incoming_rp = self.setup_incoming_rp(ap)
        for aggregation in ap.aggregation_methods:
            aggregation_method = self._get_aggregation_method(aggregation)
            for rule in sorted(ap.definition, key=lambda k: k['granularity']):

                retention = int(rule['timespan'])
                rp_name = 'rp_%s' % int(retention / 10e8)

                if self.influxdb_disable_retention_policies:
                    retention = 'INF'
                elif retention < 3600:
                    # Can't have a retention policy < 1 hour in influxDB
                    retention = '3600s'
                else:
                    retention = "%ss" % int(retention / 10e8)

                self.influx.create_retention_policy(name=rp_name,
                                                    duration=retention,
                                                    replication=1)
                granularity = int(rule['granularity'])
                measure = self._get_measurement_name(ap, aggregation,
                                                     granularity)
                granularity = int(granularity / 10e8)
                if reset:
                    try:
                        self._query('DROP MEASUREMENT %s' % measure)
                    except influxdb.exceptions.InfluxDBClientError:
                        # Already gone so ignore
                        pass

                    self._query('DROP CONTINUOUS QUERY %s ON %s' % (
                        measure, self.database))

                cq_query = self.CQ_QUERY % dict(
                    database=self.database,
                    retention=rp_name,
                    measure=measure,
                    aggregation=aggregation,
                    aggregation_method=aggregation_method,
                    granularity=granularity,
                    parent_retention=incoming_rp,
                    parent_measure=incoming_measure
                )
                try:
                    self._query(cq_query)
                except influxdb.exceptions.InfluxDBClientError:
                    # Already exists so ignore
                    pass
        if reset:
            self._backfill_data(ap=ap)

    def setup_incoming_rp(self, ap):
        # Set up an archive policy for incoming measures
        incoming_rp = self._get_incoming_rp_name(ap)
        self.influx.create_retention_policy(name=incoming_rp, duration='INF',
                                            replication=1)
        return incoming_rp

    def _create_database(self):
        self.influx.create_database(self.database)

    def _drop_database(self):
        self.influx.drop_database(self.database)

    def upgrade(self, sacks=0):
        index = indexer.get_driver(self.conf._conf)
        archive_policies = index.list_archive_policies()
        for ap in archive_policies:
            self.setup_archive_policy(ap, reset=True)

    @staticmethod
    def _get_aggregation_method(aggregation):
        if aggregation == 'std':
            return 'STDDEV(value)'
        elif aggregation.endswith('pct'):
            return 'PERCENTILE(value,%s)' % aggregation.split('p')[0]
        else:
            return '%s(value)' % aggregation

    def _get_incoming_rp_name(self, ap):
        return "rp_%s_incoming" % self._sanitize_ap_name(ap.name)

    def _get_incoming_measurement(self, ap):
        return "%s_%s" % (influxdb_common.MEASUREMENT_PREFIX,
                          self._sanitize_ap_name(ap.name))

    def _get_measurement_name(self, ap, aggregation, granularity):
        return "%s_%s_%s_%s" % (influxdb_common.MEASUREMENT_PREFIX,
                                self._sanitize_ap_name(ap.name),
                                aggregation,
                                int(int(granularity) / 10e8))

    @staticmethod
    def _sanitize_ap_name(name):
        return name.replace('-', '')

    def _earliest_time(self, measurement, rp=None, metrics=None):
        """Find the earliest point for a metric"""
        where = ""
        if metrics:
            metrics_or = " OR ".join(
                ["metric_id = '%s'" % self._get_metric_id(metric)
                 for metric in metrics])
            where = "WHERE " + metrics_or
        if rp:
            measurement_select = "%s.%s" % (rp, measurement)
        else:
            measurement_select = measurement
        query = "SELECT * FROM %s %s ORDER BY time ASC LIMIT 1" % (
            measurement_select, where)
        result = self._query(query)
        if not result:
            return None
        return list(result[measurement])[0]['time']

    def _backfill_data(self, ap):
        """Backfills data for that is outside CQ window.

        This method is really only used for tests. InfluxDB continous queries
        by default only backfill data as old as the granularity (based on the
        current time) for perfomance.
        reasons.

        This method is also run when changing an archive policy in order to
        resample what ever is in incoming data.
        """
        measurement = self._get_incoming_measurement(ap)
        rp = self.setup_incoming_rp(ap)
        result = self._query(
            "SELECT * FROM %s.%s ORDER BY time ASC LIMIT 1" % (rp,
                                                               measurement))
        if not result:
            return
        start_time = self._earliest_time(measurement, rp=rp)
        if not start_time:
            return
        cq_result = self._query('SHOW CONTINUOUS QUERIES')
        cqs = list(cq_result[self.database])

        for cq in cqs:
            items = re.search(
                '.*BEGIN\s(?P<query>.*)\s(?P<query_end>GROUP BY.*)END.*',
                cq['query']).groupdict()
            cq_name = cq['name']
            if measurement and cq_name.startswith(measurement):
                query = items.get('query') + " WHERE time >= '%s' " % \
                    start_time + items.get('query_end')
                self._query(query)

    def process_background_tasks(self, index, metrics, sync=True):
        # This is here solely for running tests and in normal operation
        # is never called.
        archive_policies = index.list_archive_policies()
        for ap in archive_policies:
            self._backfill_data(ap)

    def process_new_measures(self, indexer, incoming, metrics_to_process,
                             sync=False):
        pass

    def refresh_metric(self, indexer, incoming, metric, timeout):
        pass

    def expunge_metrics(self, incoming, index, sync=False):
        pass

    @staticmethod
    def _get_metric_id(metric):
        return str(metric.id)

    def _query(self, query):
        LOG.debug('INFLUX Query %s', query)
        result = self.influx.query(query, database=self.database)
        LOG.debug('INFLUX Result %s', result)
        return result

    def delete_metric(self, metric, sync=None):
        metric_id = self._get_metric_id(metric)

        for aggregation in metric.archive_policy.aggregation_methods:
            for definition in metric.archive_policy.definition:
                measure = self._get_measurement_name(metric.archive_policy,
                                                     aggregation,
                                                     definition.granularity)

                rp = "rp_%s" % int(definition.timespan)
                query = ("DELETE FROM "
                         "%(database)s.\"%(rp)s\".%(measure)s WHERE "
                         "metric_id='%(metric_id)s'"
                         % dict(database=self.database,
                                rp=rp,
                                measure=measure,
                                metric_id=metric_id))
                try:
                    self._query(query)
                except influxdb.client.InfluxDBClientError:
                    # Most likely gone or never downsampled so ignore
                    pass

    def get_measures(self, metric, granularities, from_timestamp=None,
                     to_timestamp=None, aggregation='mean', resample=None):
        results = []
        for g in granularities:
            results += self._get_measures(metric, g, from_timestamp,
                                         to_timestamp, aggregation)
        return results

    def _get_measures(self, metric, granularity, from_timestamp=None,
                     to_timestamp=None, aggregation='mean', resample=None):
        if aggregation not in metric.archive_policy.aggregation_methods:
            return []

        metric_id = self._get_metric_id(metric)

        results = []
        defs = sorted(
            (d
             for d in metric.archive_policy.definition
             if granularity is None or granularity == d.granularity),
            key=operator.attrgetter('granularity'))

        if not defs:
            raise ValueError("Granularity %s does not exist for %s" % (granularity, metric))

        for definition in sorted(defs, key=lambda k: k['granularity'],
                                 reverse=True):
            time_query = self._make_time_query(
                from_timestamp,
                to_timestamp,
                definition.granularity)
            if time_query:
                time_query = " AND " + time_query

            measure = self._get_measurement_name(metric.archive_policy,
                                                 aggregation,
                                                 definition.granularity)
            rp = "rp_%s" % int((int(definition.timespan) / 10e8))

            query = ("SELECT value as \"%(aggregation)s\" FROM "
                     "%(database)s.%(rp)s.%(measure)s WHERE "
                     "metric_id='%(metric_id)s' %(times)s fill(null) "
                     "ORDER BY time DESC LIMIT %(points)d"
                     % dict(database=self.database,
                            rp=rp,
                            measure=measure,
                            aggregation=aggregation,
                            metric_id=metric_id,
                            times=time_query,
                            points=definition.points))

            result = self._query(query)

            subresults = []
            for point in result[measure]:
                timestamp = self._timestamp_to_utc(
                    timeutils.parse_isotime(point['time']))
                if point[aggregation] is not None:
                    subresults.insert(0, (timestamp,
                                          definition.granularity,
                                          point[aggregation]))
            results.extend(subresults)

        return list(results)

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean',
                     granularity=None):
        results = {}
        predicate = storage.MeasureQuery(query)

        for metric in metrics:
            measures = self.get_measures(metric, from_timestamp, to_timestamp,
                                         aggregation, granularity=None)
            results[metric] = [
                (timestamp, gran, value)
                for timestamp, gran, value in measures
                if predicate(value)]
        return results

    @staticmethod
    def _timestamp_to_utc(ts):
        return timeutils.normalize_time(ts).replace(tzinfo=iso8601.iso8601.UTC)

    def _make_time_query(self, from_timestamp, to_timestamp, granularity):
        if from_timestamp:
            from_timestamp = carbonara.round_timestamp(
                from_timestamp, granularity)
            left_time = "time >= '%sZ'" % from_timestamp
        else:
            left_time = ""

        if to_timestamp:
            to_timestamp = to_timestamp
            right_time = "%sZ" % to_timestamp
        else:
            right_time = None
        if from_timestamp and to_timestamp:
            if to_timestamp <= from_timestamp:
                right_time = None
            else:
                left_time = left_time + " AND "

        return ("%s" % left_time) + ("time < '%s'" % right_time
                                     if right_time else "")

    def _get_measures_timeserie(self, metric, aggregation,
                                from_timestamp=None, to_timestamp=None):
        timeseries = self.get_measures(metric, from_timestamp=from_timestamp,
                                       to_timestamp=to_timestamp, aggregation=aggregation.method,
                                       granularities=[aggregation.granularity])
        times = []
        values = []
        if timeseries:
            for s in timeseries:
                times.append(numpy.datetime64(s[0]))
                values.append(s[2])

        return carbonara.AggregatedTimeSerie.from_data(
            sampling=aggregation.granularity,
            aggregation_method=aggregation.method,
            timestamps=times, values=values)

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  reaggregation=None, resample=None,
                                  granularity=None,
                                  needed_overlap=100.0,
                                  fill=None):
        super(InfluxDBStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp, aggregation, reaggregation,
            resample, granularity, needed_overlap)

        if reaggregation is None:
            reaggregation = aggregation

        archive_policies = set(
            [metric.archive_policy.name for metric in metrics])
        if len(archive_policies) != 1:
            raise storage.MetricUnaggregatable(metrics, 'No granularity match')

        archive_policy = metrics[0].archive_policy
        results = []
        defs = sorted(
            (d
             for d in archive_policy.definition
             if granularity is None or granularity == d.granularity),
            key=operator.attrgetter('granularity'), reverse=True)

        if not defs:
            raise storage.GranularityDoesNotExist(metric[0], granularity)

        for definition in defs:
            rp = "rp_%s" % int(definition.timespan)
            measure = self._get_measurement_name(archive_policy,
                                                 aggregation,
                                                 definition.granularity)
            earliest_time = None
            if not from_timestamp:
                earliest_time = self._earliest_time(measure, rp, metrics)
                if not earliest_time:
                    continue
                earliest_time = self._timestamp_to_utc(
                    timeutils.parse_isotime(earliest_time))
            else:
                earliest_time = from_timestamp
            time_query = self._make_time_query(
                earliest_time,
                to_timestamp,
                definition.granularity)
            if time_query:
                time_query = " AND " + time_query

            i_aggregation = self._get_aggregation_method(reaggregation)
            metrics_or = " OR ".join(
                ["metric_id = '%s'" % self._get_metric_id(metric)
                 for metric in metrics])
            query = ("SELECT %(i_aggregation)s FROM "
                     "%(database)s.%(rp)s.%(measure)s WHERE "
                     "%(metrics)s %(times)s GROUP BY time(%(granularity)ds) "
                     "fill(none) ORDER BY time DESC LIMIT %(points)d"
                     % dict(database=self.database,
                            rp=rp,
                            measure=measure,
                            aggregation=aggregation,
                            i_aggregation=i_aggregation,
                            metrics=metrics_or,
                            times=time_query,
                            granularity=definition.granularity,
                            points=definition.points))

            result = self._query(query)

            subresults = []
            for point in result[measure]:
                timestamp = self._timestamp_to_utc(
                    timeutils.parse_isotime(point['time']))
                if point[reaggregation] is not None:
                    subresults.insert(0, (timestamp,
                                          definition.granularity,
                                          point[reaggregation]))
            results.extend(subresults)

        return list(results)

    @staticmethod
    def list_metric_with_measures_to_process(size, part, full=False):
        return []
