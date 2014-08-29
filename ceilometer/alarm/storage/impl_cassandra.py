#
# Copyright 2014 Nebula, Inc
# Copyright 2014 Justin Santa Barbara
#
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
"""Cassandra storage backend
"""
import json

import ceilometer
from ceilometer.alarm.storage import base
from ceilometer.alarm.storage import models
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.storage.cassandra import connection
from ceilometer.storage.cassandra import migration
from ceilometer.storage.cassandra import utils as cassandra_utils
from ceilometer import utils

LOG = log.getLogger(__name__)

AVAILABLE_CAPABILITIES = {
    'alarms': {'query': {'simple': True,
                         'complex': False},
               'history': {'query': {'simple': True,
                                     'complex': False}}},
}

AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': True},
}


def _encode_json(o, key):
    v = o.get(key)
    if v is None:
        return
    encoded = json.dumps(v)
    o[key] = encoded


def _from_json(v):
    if v is None:
        return None
    return json.loads(v)


KEYSPACE = connection.CassandraKeyspace("alarms")

ALARM_TABLE = connection.CassandraTable(
    KEYSPACE,
    'alarm',
    ['alarm_id'],
    {
        'alarm_id': 'text',
        'type': 'text',
        'name': 'text',
        'description': 'text',
        'enabled': 'boolean',
        'state': 'text',
        'rule': 'text',
        'user_id': 'text',
        'project_id': 'text',
        'evaluation_periods': 'int',
        'period': 'int',
        'time_constraints': 'text',
        'timestamp': 'timestamp',
        'state_timestamp': 'timestamp',
        'ok_actions': 'text',
        'alarm_actions': 'text',
        'insufficient_data_actions': 'text',
        'repeat_actions': 'boolean'
    }
)

# TODO(justinsb): Shard e.g. on date?
ALARM_HISTORY_TABLE = connection.CassandraTable(
    KEYSPACE,
    'alarm_history',
    ['alarm_id', 'event_id'],
    {
        'alarm_id': 'text',
        'event_id': 'timeuuid',
        'type': 'text',
        'detail': 'text',
        'user_id': 'text',
        'project_id': 'text',
        'on_behalf_of': 'text'
    }
)


class Connection(base.Connection):
    """Stores alarm data in C*."""

    CAPABILITIES = utils.update_nested(base.Connection.CAPABILITIES,
                                       AVAILABLE_CAPABILITIES)
    STORAGE_CAPABILITIES = utils.update_nested(
        base.Connection.STORAGE_CAPABILITIES,
        AVAILABLE_STORAGE_CAPABILITIES,
    )

    def __init__(self, url):
        self.url = url
        pool = connection.CassandraConnectionPool.get_default()
        self._connection_pool = pool

    def _lease_connection(self):
        return self._connection_pool.get(self.url)

    def upgrade(self):
        with self._lease_connection() as c:
            m = migration.CassandraMigration(c.connection, KEYSPACE)
            m.run()

    def clear(self):
        with self._lease_connection() as c:
            m = migration.CassandraMigration(c.connection, KEYSPACE)
            m.clear()

    @staticmethod
    def _row_to_alarm_model(row):
        return models.Alarm(alarm_id=row.alarm_id,
                            enabled=row.enabled,
                            type=row.type,
                            name=row.name,
                            description=row.description,
                            timestamp=row.timestamp,
                            user_id=row.user_id,
                            project_id=row.project_id,
                            state=row.state,
                            state_timestamp=row.state_timestamp,
                            ok_actions=_from_json(row.ok_actions),
                            alarm_actions=_from_json(row.alarm_actions),
                            insufficient_data_actions=_from_json(
                                row.insufficient_data_actions),
                            rule=_from_json(row.rule),
                            time_constraints=_from_json(row.time_constraints),
                            repeat_actions=row.repeat_actions)

    def update_alarm(self, alarm):
        """Create an alarm.

        :param alarm: The alarm to create/update.
        """

        LOG.debug(_("Updating alarm: %s"), alarm)

        alarm_id = alarm.alarm_id
        row = alarm.as_dict()

        _encode_json(row, 'ok_actions')
        _encode_json(row, 'alarm_actions')
        _encode_json(row, 'insufficient_data_actions')
        _encode_json(row, 'rule')
        _encode_json(row, 'time_constraints')

        with self._lease_connection() as c:
            c.connection.insert(ALARM_TABLE, row)

            # TODO(justinsb): Do we really need to read it back?
            stored_alarm = c.connection.select(ALARM_TABLE,
                                               {'alarm_id': alarm_id}, limit=1)
            for alarm in stored_alarm:
                return self._row_to_alarm_model(alarm)
            LOG.warning(_("Could not find alarm immediately after update: %s"),
                        alarm)
            return None

    create_alarm = update_alarm

    def delete_alarm(self, alarm_id):
        with self._lease_connection() as c:
            c.connection.delete(KEYSPACE, ALARM_TABLE,
                                {'alarm_id': alarm_id})

    def get_alarms(self, name=None, user=None, state=None, meter=None,
                   project=None, enabled=None, alarm_id=None, pagination=None):
        if pagination:
            raise ceilometer.NotImplementedError('Pagination not implemented')
        if meter:
            raise ceilometer.NotImplementedError('Filter by meter '
                                                 'not implemented')

        q = {'alarm_id': alarm_id,
             'name': name,
             'enabled': enabled,
             'user_id': user,
             'project_id': project,
             'state': state}

        rows = []
        with self._lease_connection() as c:
            for row in c.connection.select(ALARM_TABLE, q):
                rows.append(self._row_to_alarm_model(row))

        # We need to return these sorted by alarm_id (sadly)
        rows.sort(key=lambda row: row.alarm_id)

        return rows

    def get_alarm_changes(self, alarm_id, on_behalf_of,
                          user=None, project=None, type=None,
                          start_timestamp=None, start_timestamp_op=None,
                          end_timestamp=None, end_timestamp_op=None):

        q = {'alarm_id': alarm_id,
             'on_behalf_of': on_behalf_of,
             'user_id': user,
             'project_id': project,
             'type': type}

        ts_range = cassandra_utils.make_timestamp_range(start_timestamp,
                                                        end_timestamp,
                                                        start_timestamp_op,
                                                        end_timestamp_op)
        if ts_range:
            q['event_id'] = ts_range

        rows = []
        with self._lease_connection() as c:
            for row in c.connection.select(ALARM_HISTORY_TABLE, q):
                rows.append(models.AlarmChange(
                    event_id=row.event_id.hex,
                    alarm_id=row.alarm_id,
                    type=row.type,
                    detail=row.detail,
                    user_id=row.user_id,
                    project_id=row.project_id,
                    on_behalf_of=row.on_behalf_of,
                    timestamp=cassandra_utils.timeuuid_to_timestamp(
                        row.event_id)))

        # We need to return these sorted by recentness (sadly)
        rows.sort(key=lambda row: row.timestamp)
        rows.reverse()

        return rows

    def record_alarm_change(self, alarm_change):
        """Record alarm change event."""
        # ts = alarm_change.get('timestamp') or datetime.datetime.now()
        row = {}
        for k, v in alarm_change.iteritems():
            if k == 'timestamp':
                row['event_id'] = cassandra_utils.timeuuid_from_timestamp(v)
            else:
                row[k] = v

        with self._lease_connection() as c:
            c.connection.insert(ALARM_HISTORY_TABLE, row)
