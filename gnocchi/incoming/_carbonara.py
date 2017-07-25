# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
from concurrent import futures

import daiquiri
import numpy
import six

from gnocchi import incoming
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)

_NUM_WORKERS = utils.get_default_workers()


class CarbonaraBasedStorage(incoming.StorageDriver):
    MEASURE_PREFIX = "measure"
    SACK_PREFIX = "incoming"
    CFG_PREFIX = 'gnocchi-config'
    CFG_SACKS = 'sacks'

    @property
    def NUM_SACKS(self):
        if not hasattr(self, '_num_sacks'):
            try:
                self._num_sacks = int(self.get_storage_sacks())
            except Exception as e:
                LOG.error('Unable to detect the number of storage sacks. '
                          'Ensure gnocchi-upgrade has been executed: %s', e)
                raise incoming.SackDetectionError(e)
        return self._num_sacks

    def get_sack_prefix(self, num_sacks=None):
        sacks = num_sacks if num_sacks else self.NUM_SACKS
        return self.SACK_PREFIX + str(sacks) + '-%s'

    def upgrade(self, num_sacks):
        super(CarbonaraBasedStorage, self).upgrade()
        if not self.get_storage_sacks():
            self.set_storage_settings(num_sacks)

    @staticmethod
    def get_storage_sacks():
        """Return the number of sacks in storage. None if not set."""
        raise NotImplementedError

    @staticmethod
    def set_storage_settings(num_sacks):
        raise NotImplementedError

    @staticmethod
    def remove_sack_group(num_sacks):
        raise NotImplementedError

    @staticmethod
    def get_sack_lock(coord, sack):
        lock_name = b'gnocchi-sack-%s-lock' % str(sack).encode('ascii')
        return coord.get_lock(lock_name)

    _SERIALIZE_DTYPE = [('timestamps', '<datetime64[ns]'),
                        ('values', '<d')]

    def _make_measures_array(self):
        return numpy.array([], dtype=self._SERIALIZE_DTYPE)

    @staticmethod
    def _array_concatenate(arrays):
        if arrays:
            return numpy.concatenate(arrays)
        return arrays

    def _unserialize_measures(self, measure_id, data):
        try:
            return numpy.frombuffer(data, dtype=self._SERIALIZE_DTYPE)
        except ValueError:
            LOG.error(
                "Unable to decode measure %s, possible data corruption",
                measure_id)
            raise

    def _encode_measures(self, measures):
        return numpy.array(list(measures),
                           dtype=self._SERIALIZE_DTYPE).tobytes()

    def add_measures_batch(self, metrics_and_measures):
        with futures.ThreadPoolExecutor(max_workers=_NUM_WORKERS) as executor:
            list(executor.map(
                lambda args: self._store_new_measures(*args),
                ((metric, self._encode_measures(measures))
                 for metric, measures
                 in six.iteritems(metrics_and_measures))))

    @staticmethod
    def _store_new_measures(metric, data):
        raise NotImplementedError

    def measures_report(self, details=True):
        metrics, measures, full_details = self._build_report(details)
        report = {'summary': {'metrics': metrics, 'measures': measures}}
        if full_details is not None:
            report['details'] = full_details
        return report

    @staticmethod
    def _build_report(details):
        raise NotImplementedError

    @staticmethod
    def delete_unprocessed_measures_for_metric_id(metric_id):
        raise NotImplementedError

    @staticmethod
    def process_measure_for_metric(metric):
        raise NotImplementedError

    @staticmethod
    def has_unprocessed(metric):
        raise NotImplementedError

    def sack_for_metric(self, metric_id):
        return metric_id.int % self.NUM_SACKS

    def get_sack_name(self, sack):
        return self.get_sack_prefix() % sack