# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2016 eNovance
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
import functools
import json
import os
import subprocess
import uuid

import fixtures
from oslotest import base
from oslotest import mockpatch
import six
from six.moves.urllib.parse import unquote
try:
    from swiftclient import exceptions as swexc
except ImportError:
    swexc = None
from testtools import testcase
from tooz import coordination

from gnocchi import archive_policy
from gnocchi import exceptions
from gnocchi import indexer
from gnocchi import service
from gnocchi import storage


class SkipNotImplementedMeta(type):
    def __new__(cls, name, bases, local):
        for attr in local:
            value = local[attr]
            if callable(value) and (
                    attr.startswith('test_') or attr == 'setUp'):
                local[attr] = _skip_decorator(value)
        return type.__new__(cls, name, bases, local)


def _skip_decorator(func):
    @functools.wraps(func)
    def skip_if_not_implemented(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except exceptions.NotImplementedError as e:
            raise testcase.TestSkipped(six.text_type(e))
    return skip_if_not_implemented


class FakeSwiftClient(object):
    def __init__(self, *args, **kwargs):
        self.kvs = {}

    def put_container(self, container, response_dict=None):
        if response_dict is not None:
            if container in self.kvs:
                response_dict['status'] = 204
            else:
                response_dict['status'] = 201
        self.kvs[container] = {}

    def get_container(self, container, delimiter=None,
                      path=None, full_listing=False, limit=None):
        try:
            container = self.kvs[container]
        except KeyError:
            raise swexc.ClientException("No such container",
                                        http_status=404)

        files = []
        directories = set()
        for k, v in six.iteritems(container.copy()):
            if path and not k.startswith(path):
                continue

            if delimiter is not None and delimiter in k:
                dirname = k.split(delimiter, 1)[0]
                if dirname not in directories:
                    directories.add(dirname)
                    files.append({'subdir': dirname + delimiter})
            else:
                files.append({'bytes': len(v),
                              'last_modified': None,
                              'hash': None,
                              'name': k,
                              'content_type': None})

        if full_listing:
            end = None
        elif limit:
            end = limit
        else:
            # In truth, it's 10000, but 1 is enough to make sure our test fails
            # otherwise.
            end = 1

        return ({'x-container-object-count': len(container.keys())},
                (files + list(directories))[:end])

    def put_object(self, container, key, obj):
        if hasattr(obj, "seek"):
            obj.seek(0)
            obj = obj.read()
            # TODO(jd) Maybe we should reset the seek(), but well…
        try:
            self.kvs[container][key] = obj
        except KeyError:
            raise swexc.ClientException("No such container",
                                        http_status=404)

    def get_object(self, container, key):
        try:
            return {}, self.kvs[container][key]
        except KeyError:
            raise swexc.ClientException("No such container/object",
                                        http_status=404)

    def delete_object(self, container, obj):
        try:
            del self.kvs[container][obj]
        except KeyError:
            raise swexc.ClientException("No such container/object",
                                        http_status=404)

    def delete_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)
        if self.kvs[container]:
            raise swexc.ClientException("Container not empty",
                                        http_status=409)
        del self.kvs[container]

    def head_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)

    def post_account(self, headers, query_string=None, data=None,
                     response_dict=None):
        if query_string == 'bulk-delete':
            resp = {'Response Status': '200 OK',
                    'Response Body': '',
                    'Number Deleted': 0,
                    'Number Not Found': 0}
            if response_dict is not None:
                response_dict['status'] = 200
            if data:
                for path in data.splitlines():
                    try:
                        __, container, obj = (unquote(path.decode('utf8'))
                                              .split('/', 2))
                        del self.kvs[container][obj]
                        resp['Number Deleted'] += 1
                    except KeyError:
                        resp['Number Not Found'] += 1
            return {}, json.dumps(resp).encode('utf-8')

        if response_dict is not None:
            response_dict['status'] = 204

        return {}, None


@six.add_metaclass(SkipNotImplementedMeta)
class TestCase(base.BaseTestCase):

    ARCHIVE_POLICIES = {
        'no_granularity_match': archive_policy.ArchivePolicy(
            "no_granularity_match",
            0,
            [
                # 2 second resolution for a day
                archive_policy.ArchivePolicyItem(
                    granularity=2, points=3600 * 24),
                ],
        ),
    }

    @staticmethod
    def path_get(project_file=None):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..',
                                            )
                               )
        if project_file:
            return os.path.join(root, project_file)
        return root

    @classmethod
    def setUpClass(self):
        super(TestCase, self).setUpClass()
        self.conf = service.prepare_service([],
                                            default_config_files=[])
        self.conf.set_override('policy_file',
                               self.path_get('etc/gnocchi/policy.json'),
                               group="oslo_policy")

        # NOTE(jd) This allows to test S3 on AWS
        if not os.getenv("AWS_ACCESS_KEY_ID"):
            self.conf.set_override('s3_endpoint_url',
                                   os.getenv("GNOCCHI_STORAGE_HTTP_URL"),
                                   group="storage")
            self.conf.set_override('s3_access_key_id', "gnocchi",
                                   group="storage")
            self.conf.set_override('s3_secret_access_key', "anythingworks",
                                   group="storage")

        self.index = indexer.get_driver(self.conf)
        self.index.connect()

        # NOTE(jd) So, some driver, at least SQLAlchemy, can't create all
        # their tables in a single transaction even with the
        # checkfirst=True, so what we do here is we force the upgrade code
        # path to be sequential to avoid race conditions as the tests run
        # in parallel.
        self.coord = coordination.get_coordinator(
            self.conf.storage.coordination_url,
            str(uuid.uuid4()).encode('ascii'))

        self.coord.start(start_heart=True)

        with self.coord.get_lock(b"gnocchi-tests-db-lock"):
            self.index.upgrade()

        self.coord.stop()

        self.archive_policies = self.ARCHIVE_POLICIES.copy()
        self.archive_policies.update(archive_policy.DEFAULT_ARCHIVE_POLICIES)
        for name, ap in six.iteritems(self.archive_policies):
            # Create basic archive policies
            try:
                self.index.create_archive_policy(ap)
            except indexer.ArchivePolicyAlreadyExists:
                pass

        storage_driver = os.getenv("GNOCCHI_TEST_STORAGE_DRIVER", "file")
        self.conf.set_override('driver', storage_driver, 'storage')
        if storage_driver == 'ceph':
            self.conf.set_override('ceph_conffile',
                                   os.getenv("CEPH_CONF"),
                                   'storage')

    def setUp(self):
        super(TestCase, self).setUp()
        if swexc:
            self.useFixture(mockpatch.Patch(
                'swiftclient.client.Connection',
                FakeSwiftClient))

        if self.conf.storage.driver == 'file':
            tempdir = self.useFixture(fixtures.TempDir())
            self.conf.set_override('file_basepath',
                                   tempdir.path,
                                   'storage')
        elif self.conf.storage.driver == 'ceph':
            pool_name = uuid.uuid4().hex
            subprocess.call("rados -c %s mkpool %s" % (
                os.getenv("CEPH_CONF"), pool_name), shell=True)
            self.conf.set_override('ceph_pool', pool_name, 'storage')

        if self.conf.storage.driver == 'influxdb':
            db_name = 'gnocchitest%s' % \
                      str(uuid.uuid4()).encode('ascii').replace('-', '')
            self.conf.set_override('influxdb_database',
                                   db_name,
                                   'storage')
            self.conf.set_override('influxdb_port',
                                   os.getenv("GNOCCHI_STORAGE_INFLUXDB_PORT",
                                             "8086"),
                                   'storage')
            self.conf.set_override('influxdb_disable_retention_policies',
                                   True,
                                   'storage')

        self.storage = storage.get_driver(self.conf)
        # NOTE(jd) Do not upgrade the storage. We don't really need the storage
        # upgrade for now, and the code that upgrade from pre-1.3
        # (TimeSerieArchive) uses a lot of parallel lock, which makes tooz
        # explodes because MySQL does not support that many connections in real
        # life.
        # self.storage.upgrade(self.index)

    def tearDown(self):
        self.index.disconnect()
        self.storage.stop()
        if self.conf.storage.driver == 'influxdb':
            self.storage.drop_db()
        super(TestCase, self).tearDown()
