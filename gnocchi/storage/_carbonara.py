# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import collections
import logging
import multiprocessing
import uuid

from concurrent import futures
import iso8601
from oslo_config import cfg
from oslo_serialization import msgpackutils
from oslo_utils import timeutils
import six
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage

OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               help='Number of workers to run during adding new measures for '
                    'pre-aggregation needs.'),
    cfg.StrOpt('coordination_url',
               secret=True,
               help='Coordination driver URL',
               default="file:///var/lib/gnocchi/locks"),

]

LOG = logging.getLogger(__name__)


class CarbonaraBasedStorage(storage.StorageDriver):
    MEASURE_PREFIX = "measure"

    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self.coord = coordination.get_coordinator(
            conf.coordination_url,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()
        if conf.aggregation_workers_number is None:
            try:
                self.aggregation_workers_number = multiprocessing.cpu_count()
            except NotImplementedError:
                self.aggregation_workers_number = 2
        else:
            self.aggregation_workers_number = conf.aggregation_workers_number

    def stop(self):
        self.coord.stop()

    def _lock(self, metric):
        lock_name = b"gnocchi-" + str(metric.id).encode('ascii')
        return self.coord.get_lock(lock_name)

    @staticmethod
    def _get_measures(metric, aggregation, granularity):
        raise NotImplementedError

    @staticmethod
    def _get_unaggregated_timeserie(metric):
        raise NotImplementedError

    @staticmethod
    def _store_unaggregated_timeserie(metric, data):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, aggregation, granularity, data):
        raise NotImplementedError

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None):
        super(CarbonaraBasedStorage, self).get_measures(
            metric, from_timestamp, to_timestamp, aggregation)
        if granularity is None:
            agg_timeseries = self._map_in_thread(
                self._get_measures_timeserie,
                ((metric, aggregation, ap.granularity)
                 for ap in reversed(metric.archive_policy.definition)))
        else:
            agg_timeseries = [self._get_measures_timeserie(
                metric, aggregation, granularity)]
        return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                for ts in agg_timeseries
                for timestamp, r, v in ts.fetch(from_timestamp, to_timestamp)]

    @staticmethod
    def _log_data_corruption(metric, aggregation):
        LOG.error("Data are corrupted for metric %(metric)s and aggregation "
                  "%(aggregation)s, recreating an empty timeserie." %
                  dict(metric=metric.id, aggregation=aggregation))

    def _get_measures_timeserie(self, metric, aggregation, granularity):
        try:
            contents = self._get_measures(metric, aggregation, granularity)
        except storage.MetricDoesNotExist:
            ts = None
        except storage.AggregationDoesNotExist:
            # This does not necessarily mean that the metric has not been
            # created yet. It can be an old metric with a TimeSerieArchive.
            # Let's try that.
            try:
                data = self._get_metric_archive(metric, aggregation)
            except (storage.MetricDoesNotExist,
                    storage.AggregationDoesNotExist):
                # It really does not exist
                ts = None
            else:
                archive = carbonara.TimeSerieArchive.unserialize(data)
                # It's an old metric with an TimeSerieArchive!
                for ts in archive.agg_timeseries:
                    if ts.sampling == granularity:
                        return ts
                raise storage.GranularityDoesNotExist(metric, granularity)
        else:
            try:
                ts = carbonara.AggregatedTimeSerie.unserialize(contents)
            except ValueError:
                self._log_data_corruption(metric, aggregation)
                ts = None

        if ts is None:
            for d in metric.archive_policy.definition:
                if d.granularity == granularity:
                    ts = carbonara.AggregatedTimeSerie(
                        aggregation_method=aggregation,
                        sampling=granularity,
                        max_size=d.points)
                    break
            else:
                raise storage.GranularityDoesNotExist(metric, granularity)
        return ts

    def _add_measures(self, aggregation, granularity, metric, timeserie):
        ts = self._get_measures_timeserie(metric, aggregation, granularity)
        ts.update(timeserie)
        self._store_metric_measures(metric, aggregation, granularity,
                                    ts.serialize())

    def add_measures(self, metric, measures):
        self._store_measures(metric, msgpackutils.dumps(
            list(map(tuple, measures))))

    @staticmethod
    def _store_measures(metric, data):
        raise NotImplementedError

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    @staticmethod
    def _list_metric_with_measures_to_process(metric_id):
        raise NotImplementedError

    @staticmethod
    def _pending_measures_to_process_count(metric_id):
        raise NotImplementedError

    def delete_metric(self, metric):
        with self._lock(metric):
            # If the metric has never been upgraded, we need to delete this
            # here too
            self._delete_metric_archives(metric)
            self._delete_metric(metric)

    @staticmethod
    def _unserialize_measures(data):
        return msgpackutils.loads(data)

    def measures_report(self):
        metrics_to_process = self._list_metric_with_measures_to_process()
        return dict(
            (metric_id, self._pending_measures_to_process_count(metric_id))
            for metric_id in metrics_to_process)

    def _check_for_metric_upgrade(self, metric):
        lock = self._lock(metric)
        with lock:
            for agg_method in metric.archive_policy.aggregation_methods:
                LOG.debug(
                    "Checking if the metric %s needs migration for %s"
                    % (metric, agg_method))
                try:
                    data = self._get_metric_archive(metric, agg_method)
                except storage.MetricDoesNotExist:
                    # Just try the next metric, this one has no measures
                    break
                except storage.AggregationDoesNotExist:
                    # This should not happen, but you never know.
                    LOG.warn(
                        "Metric %s does not have an archive "
                        "for aggregation %s, "
                        "no migration can be done" % (metric, agg_method))
                else:
                    LOG.info("Migrating metric %s to new format" % metric)
                    archive = carbonara.TimeSerieArchive.unserialize(data)
                    for ts in archive.agg_timeseries:
                        # Store each AggregatedTimeSerie independently
                        self._store_metric_measures(
                            metric, ts.aggregation_method,
                            ts.sampling,
                            ts.serialize())
            self._delete_metric_archives(metric)
            LOG.info("Migrated metric %s to new format" % metric)

    def upgrade(self, index):
        self._map_in_thread(
            self._check_for_metric_upgrade,
            ((metric,) for metric in index.list_metrics()))

    def process_measures(self, indexer, sync=False):
        metrics_to_process = self._list_metric_with_measures_to_process()
        metrics = indexer.get_metrics(metrics_to_process)
        # This build the list of deleted metrics, i.e. the metrics we have
        # measures to process for but that are not in the indexer anymore.
        deleted_metrics_id = (set(map(uuid.UUID, metrics_to_process))
                              - set(m.id for m in metrics))
        for metric_id in deleted_metrics_id:
            self._delete_unprocessed_measures_for_metric_id(metric_id)
        for metric in metrics:
            lock = self._lock(metric)
            agg_methods = list(metric.archive_policy.aggregation_methods)
            # Do not block if we cannot acquire the lock, that means some other
            # worker is doing the job. We'll just ignore this metric and may
            # get back later to it if needed.
            if lock.acquire(blocking=sync):
                try:
                    LOG.debug("Processing measures for %s" % metric)
                    with self._process_measure_for_metric(metric) as measures:
                        try:
                            with timeutils.StopWatch() as sw:
                                raw_measures = (
                                    self._get_unaggregated_timeserie(
                                        metric)
                                )
                                LOG.debug(
                                    "Retrieve unaggregated measures "
                                    "for %s in %.2fs"
                                    % (metric.id, sw.elapsed()))
                        except storage.MetricDoesNotExist:
                            try:
                                self._create_metric(metric)
                            except storage.MetricAlreadyExists:
                                # Created in the mean time, do not worry
                                pass
                            ts = None
                        else:
                            try:
                                ts = carbonara.BoundTimeSerie.unserialize(
                                    raw_measures)
                            except ValueError:
                                ts = None
                                self._log_data_corruption(metric, "none")

                        if ts is None:
                            # This is the first time we treat measures for this
                            # metric, or data are corrupted, create a new one
                            mbs = metric.archive_policy.max_block_size
                            ts = carbonara.BoundTimeSerie(
                                block_size=mbs,
                                back_window=metric.archive_policy.back_window)

                        def _map_add_measures(bound_timeserie):
                            self._map_in_thread(
                                self._add_measures,
                                ((aggregation, d.granularity,
                                 metric, bound_timeserie)
                                 for aggregation in agg_methods
                                 for d in metric.archive_policy.definition))

                        with timeutils.StopWatch() as sw:
                            ts.set_values(
                                measures,
                                before_truncate_callback=_map_add_measures,
                                ignore_too_old_timestamps=True)
                            LOG.debug(
                                "Computed new metric %s with %d new measures "
                                "in %.2f seconds"
                                % (metric.id, len(measures), sw.elapsed()))

                        self._store_unaggregated_timeserie(metric,
                                                           ts.serialize())
                except Exception:
                    if sync:
                        raise
                    LOG.error("Error processing new measures", exc_info=True)
                finally:
                    lock.release()

    # TODO(jd) Add granularity parameter here and in the REST API
    # rather than fetching all granularities
    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  needed_overlap=100.0):
        super(CarbonaraBasedStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp, aggregation, needed_overlap)

        granularities = (definition.granularity
                         for metric in metrics
                         for definition in metric.archive_policy.definition)
        granularities_in_common = [
            granularity
            for granularity, occurence in six.iteritems(
                collections.Counter(granularities))
            if occurence == len(metrics)
        ]

        if not granularities_in_common:
            raise storage.MetricUnaggregatable(metrics, 'No granularity match')

        tss = self._map_in_thread(self._get_measures_timeserie,
                                  [(metric, aggregation, granularity)
                                   for metric in metrics
                                   for granularity in granularities_in_common])
        try:
            return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                    for timestamp, r, v
                    in carbonara.AggregatedTimeSerie.aggregated(
                        tss, from_timestamp, to_timestamp,
                        aggregation, needed_overlap)]
        except carbonara.UnAggregableTimeseries as e:
            raise storage.MetricUnaggregatable(metrics, e.reason)

    def _find_measure(self, metric, aggregation, granularity, predicate,
                      from_timestamp, to_timestamp):
        timeserie = self._get_measures_timeserie(
            metric, aggregation, granularity)
        values = timeserie.fetch(from_timestamp, to_timestamp)
        return {metric:
                [(timestamp.replace(tzinfo=iso8601.iso8601.UTC),
                  g, value)
                 for timestamp, g, value in values
                 if predicate(value)]}

    # TODO(jd) Add granularity parameter here and in the REST API
    # rather than fetching all granularities
    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean'):
        predicate = storage.MeasureQuery(query)
        results = self._map_in_thread(
            self._find_measure,
            [(metric, aggregation,
              ap.granularity, predicate,
              from_timestamp, to_timestamp)
             for metric in metrics
             for ap in metric.archive_policy.definition])
        result = collections.defaultdict(list)
        for r in results:
            for metric, metric_result in six.iteritems(r):
                result[metric].extend(metric_result)

        # Sort the result
        for metric, r in six.iteritems(result):
            # Sort by timestamp asc, granularity desc
            r.sort(key=lambda t: (t[0], - t[1]))

        return result

    def _map_in_thread(self, method, list_of_args):
        with futures.ThreadPoolExecutor(
                max_workers=self.aggregation_workers_number) as executor:
            # We use 'list' to iterate all threads here to raise the first
            # exception now, not much choice
            return list(executor.map(lambda args: method(*args), list_of_args))
