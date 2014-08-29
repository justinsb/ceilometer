#
# Copyright 2014 Nebula, Inc
# Copyright 2014 Justin Santa Barbara
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
"""Tests for ceilometer/storage/impl_cassandra.py"""

from ceilometer.alarm.storage import impl_cassandra as cassandra_alarm
from ceilometer.storage import impl_cassandra as cassandra_main
from ceilometer.tests import base as test_base


class CapabilitiesTest(test_base.BaseTestCase):
    # Check the returned capabilities list, which is specific to each DB
    # driver

    def test_capabilities(self):
        expected = {
            'meters': {'pagination': False,
                       'query': {'simple': True,
                                 'metadata': True,
                                 'complex': False}},
            'resources': {'pagination': False,
                          'query': {'simple': True,
                                    'metadata': True,
                                    'complex': False}},
            'samples': {'pagination': False,
                        'groupby': False,
                        'query': {'simple': True,
                                  'metadata': True,
                                  'complex': False}},
            'statistics': {'pagination': False,
                           'groupby': False,
                           'query': {'simple': True,
                                     'metadata': True,
                                     'complex': False},
                           'aggregation': {'standard': True,
                                           'selectable': {
                                               'max': False,
                                               'min': False,
                                               'sum': False,
                                               'avg': False,
                                               'count': False,
                                               'stddev': False,
                                               'cardinality': False}}
                           },
        }

        actual = cassandra_main.Connection.get_capabilities()
        self.assertEqual(expected, actual)

    def test_alarm_capabilities(self):
        expected = {
            'alarms': {'query': {'simple': True,
                                 'complex': False},
                       'history': {'query': {'simple': True,
                                             'complex': False}}},
        }

        actual = cassandra_alarm.Connection.get_capabilities()
        self.assertEqual(expected, actual)

    # def test_event_capabilities(self):
    #     expected = {
    #         'events': {'query': {'simple': True}},
    #     }
    #
    #     actual = hbase_event.Connection.get_capabilities()
    #     self.assertEqual(expected, actual)

    def test_storage_capabilities(self):
        expected = {
            'storage': {'production_ready': False},
        }
        actual = cassandra_main.Connection.get_storage_capabilities()
        self.assertEqual(expected, actual)
