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
"""Create/destroy tables & indexes in Cassandra."""

import json

from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)


class CassandraMigration():
    """Creates schema in C* from models."""

    def __init__(self, connection, keyspace):
        self.connection = connection
        self.keyspace = keyspace
        self._ddl_timeout = 300

    def run(self):
        self._migrate_keyspace()
        for table in self.keyspace.tables:
            self._migrate_table(table)
            for _, secondary_index in table.secondary_indexes.iteritems():
                self._migrate_index(secondary_index)

    def clear(self):
        timeout = self._ddl_timeout
        self.connection.drop_keyspace(self.keyspace, timeout=timeout)

    def _migrate_keyspace(self):
        keyspace = self.keyspace
        # XXX: Make configurable?
        replication = {}
        replication['class'] = 'SimpleStrategy'
        replication['replication_factor'] = "1"
        keyspace_name = self.connection.keyspace_prefix + keyspace.name
        cql = ('CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION=%s' % (
            keyspace_name, json.dumps(replication).replace('"', "'")
        ))
        self.connection.execute(None, cql, timeout=self._ddl_timeout)

    def _migrate_table(self, table):
        columns = []
        for column in table.columns:
            columns.append("%s %s" % (column.name, column.type))
        pks = table.pks

        # Support compound partition keys
        pk_specs = (['(' + ','.join(pks[0:table.partition_key_length]) + ')']
                    + pks[table.partition_key_length:])

        cql = ('CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s))' %
               (table.escaped_name, ','.join(columns), ','.join(pk_specs)))

        if table.clustering_key:
            ck_specs = []
            for k in table.clustering_key:
                field = k['field']
                reverse = k.get('reverse', False)
                asc_or_desc = 'DESC' if reverse else 'ASC'
                ck_spec = field + ' ' + asc_or_desc
                ck_specs.append(ck_spec)
            cql = (cql +
                   ' WITH CLUSTERING ORDER BY (' + ','.join(ck_specs) + ')')

        self.connection.execute(self.keyspace, cql, timeout=self._ddl_timeout)

    def _migrate_index(self, index):
        table = index.table

        cql_columns = [c.escaped_name for c in index.columns]

        # Should be index.escaped_name,
        # but CQL parser doesn't accept the quoted form
        cql = 'CREATE INDEX IF NOT EXISTS %s ON %s (%s);' % (
            index.name,
            table.escaped_name,
            ','.join(cql_columns))

        self.connection.execute(table.keyspace, cql, timeout=self._ddl_timeout)
