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
"""This is a very crude version of 'in-memory Cassandra'.

It implements MemConnection, which is a mock for CassandraConnection."""

import calendar
import datetime
import random
import uuid

from oslo.utils import timeutils

from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.storage.cassandra import utils as cassandra_utils

LOG = log.getLogger(__name__)


def get_now():
    """Returns time in C* format."""
    t = timeutils.utcnow()
    t = calendar.timegm(t.timetuple())
    t = int(t * 1E6)
    return t


class Row(object):
    """A row in an mock C* table."""
    def __init__(self, table, data, ttl):
        self.ttl = ttl
        now = get_now()
        for k, v in data.items():
            column = table.get_column(k)
            self.__dict__[k] = column.cast_to_db(v)
            self.__dict__['writetime_' + k] = now
        for c in table.columns:
            if c.name not in self.__dict__:
                self.__dict__[c.name] = c._build_empty()
                self.__dict__['writetime_' + c.name] = now

    def update(self, table, data, merge_columns=[]):
        now = get_now()
        for k, v in data.items():
            column = table.get_column(k)
            # cv = to_cassandra(v)
            if k in merge_columns:
                if isinstance(v, dict):
                    existing = self.__dict__.get(k)
                    for k2, v2 in v.iteritems():
                        existing[k2] = v2
                else:
                    raise Exception("Can't merge value of type: %s", type(v))
            else:
                self.__dict__[k] = column.cast_to_db(v)
            self.__dict__['writetime_' + k] = now


class MemTable(object):
    """A mock C* table."""
    def __init__(self, connection, definition):
        self.connection = connection
        self.definition = definition
        self.rows = []

    def select(self, query, limit=None, order=None, extra_columns=None):
        results = []

        query = self._compile_query(query)

        for row in self.rows:
            if self._row_matches(row, query):
                results.append(row)
                if limit is not None and not order and len(results) >= limit:
                    break

        if order:
            for o in order:
                key = o['field']
                column = self.definition.get_column(key)
                reverse = o.get('reverse', False)

                def value_fn(row):
                    v = getattr(row, key, None)
                    if column.type == 'timeuuid':
                        v = cassandra_utils.timeuuid_to_timestamp(v)
                    return v

                results.sort(key=value_fn, reverse=reverse)
        else:
            # Shuffle rows, to test unspecified orderings
            random.shuffle(results)

        return results

    def insert(self, data, ttl=None):
        # In Cassandra, an insert is an update
        return self.update(data, ttl=ttl)

    def update(self, data, ttl=None, merge_columns=[]):
        row = self._find_by_pk(data)
        if row is None:
            row = Row(self.definition, data, ttl)
            self.rows.append(row)
        else:
            row.update(self.definition, data, merge_columns)

    def delete(self, query):
        query = self._compile_query(query)

        rows = self.rows
        rows = filter(lambda r: not self._row_matches(r, query), rows)
        self.rows = rows

    def _compile_query(self, query):
        compiled = {}

        for name, expected in query.iteritems():
            if expected is None:
                # None means wildcard
                continue
            column = self.definition.get_column(name)
            if isinstance(expected, dict):
                compiled[name] = {}
                for op, threshold in expected.iteritems():
                    compiled[name][op] = column.cast_to_db(threshold)
            else:
                compiled[name] = column.cast_to_db(expected)

        return compiled

    def _find_by_pk(self, data):
        pks = self.definition.pks
        q = {}
        for pk in pks:
            v = data.get(pk)
            if v is None:
                raise Exception("PK field not specified: " + pk)
            q[pk] = v
        rows = self.select(q, limit=1)
        if len(rows) == 0:
            return None
        if len(rows) == 1:
            return rows[0]
        raise Exception("Should be unreachable")

    def _row_matches(self, row, query):
        for k, expected in query.items():
            # if expected is None:
            #     # None means ignore
            #     continue
            actual = getattr(row, k, None)
            if isinstance(expected, dict):
                column = self.definition.get_column(k)
                if not self._column_matches(column, actual, expected):
                    return False
            else:
                t = type(expected)
                if t is str:
                    if isinstance(actual, str):
                        if expected != actual:
                            return False
                    elif isinstance(actual, unicode):
                        if unicode(expected) != actual:
                            return False
                    else:
                        raise Exception("Can't handle str vs non-str: %s" %
                                        type(actual))
                elif t is unicode:
                    if isinstance(actual, unicode):
                        if expected != actual:
                            return False
                    elif isinstance(actual, str):
                        if expected != unicode(actual):
                            return False
                    else:
                        raise Exception(
                            "Can't handle unicode vs non-unicode: %s" %
                            type(actual))
                elif t is int or t is bool:
                    if expected != actual:
                        return False
                elif expected is None:
                    if actual is not None:
                        return False
                elif t is datetime.datetime:
                    if isinstance(actual, datetime.datetime):
                        if expected != actual:
                            return False
                    else:
                        raise Exception("Can't handle date vs non-date: %s" %
                                        type(actual))
                elif t is uuid.UUID:
                    if isinstance(actual, uuid.UUID):
                        if expected != actual:
                            return False
                    elif isinstance(actual, str):
                        if str(expected) != actual:
                            return False
                    else:
                        raise Exception("Can't handle uuid vs non-uuid: %s" %
                                        type(actual))
                else:
                    raise Exception("Can't handle value of type: %s", t)
        return True

    def _column_matches(self, column, actual, complex):
        for op, expected in complex.iteritems():
            simple = False

            if type(expected) is int and type(actual) is int:
                simple = True
            elif (type(expected) is datetime.datetime
                  and type(actual) is datetime.datetime):
                simple = True

            if not simple:
                if column.type == 'timeuuid':
                    if (type(actual) is uuid.UUID
                       and type(expected) is datetime.datetime):
                        actual = cassandra_utils.timeuuid_to_timestamp(actual)
                        simple = True

            if simple:
                if op == '>':
                    if not actual > expected:
                        return False
                    continue
                elif op == '>=':
                    if not actual >= expected:
                        return False
                    continue
                elif op == '<=':
                    if not actual <= expected:
                        return False
                    continue
                elif op == '<':
                    if not actual < expected:
                        return False
                    continue
                else:
                    raise Exception("Unhandled operator: %s", op)
            elif op == 'in':
                    if actual not in expected:
                        return False
                    continue
            else:
                raise Exception("Can't handle operation %s %s with %s vs %s" %
                                (column.type,
                                 op,
                                 type(expected),
                                 type(actual)))
        return True


class MemKeyspace(object):
    """A mock C* keyspace."""
    def __init__(self, connection, definition):
        self.connection = connection
        self.definition = definition
        self.tables = {}

    def _get_table(self, table):
        name = table.name
        t = self.tables.get(name)
        if t is None:
            t = MemTable(self, table)
            self.tables[name] = t
        return t


class MemConnection(object):
    """A mock C* connection.

    This has the same API as CassandraConnection.
    """
    _instances = {}

    def __init__(self, keyspace_prefix):
        self.keyspaces = {}
        self.tables = {}
        self.keyspace_prefix = keyspace_prefix
        self.url = "inmemory:" + keyspace_prefix

    def close(self):
        pass

    @staticmethod
    def get(keyspace_prefix):
        k = MemConnection._instances.get(keyspace_prefix)
        if k is None:
            LOG.debug(_("Creating a new in-memory Cassandra mock with prefix: "
                        "%s"), keyspace_prefix)
            k = MemConnection(keyspace_prefix)
            MemConnection._instances[keyspace_prefix] = k
        return k

    def execute(self, keyspace_name, cql, timeout=None):
        ignore = False
        if cql.startswith("CREATE KEYSPACE "):
            ignore = True
        elif cql.startswith("CREATE TABLE "):
            ignore = True
        elif cql.startswith("CREATE INDEX "):
            ignore = True

        if ignore:
            LOG.info(_("Ignoring: %s"), cql)
            return
        raise Exception("Unhandled statement: %s", cql)

    def drop_keyspace(self, keyspace, timeout=None):
        self.keyspaces[keyspace.name] = None

    def select(self, table, query, limit=None, order=None, extra_columns=None):
        t = self._get_table(table)
        return t.select(query,
                        limit=limit,
                        order=order,
                        extra_columns=extra_columns)

    def _get_table(self, table):
        k = self._get_keyspace(table.keyspace)
        t = k._get_table(table)
        return t

    def _get_keyspace(self, keyspace):
        name = keyspace.name
        k = self.keyspaces.get(name)
        if k is None:
            k = MemKeyspace(self, keyspace)
            self.keyspaces[name] = k
        return k

    def insert(self, table, data=None, ttl=None):
        """Insert dict into specified keyspace & table."""

        t = self._get_table(table)
        ret = t.insert(data, ttl=ttl)

        for index_table in table.index_tables:
            if not index_table.on_insert:
                continue
            index_table._after_insert(self, data, ttl)
        return ret

    def update(self, table, data, merge_columns=[]):
        t = self._get_table(table)
        return t.update(data, merge_columns=merge_columns)

    def delete(self, keyspace, table, filt=None):
        t = self._get_table(table)
        return t.delete(filt)
