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
""" Cassandra connection helpers
"""
import functools
import json
import os
import time
import uuid

import cassandra
import cassandra.cluster
import cassandra.query
from oslo.utils import netutils
from six.moves.urllib import parse as urlparse

from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.storage.cassandra import eventletreactor
from ceilometer.storage.cassandra import inmemory as cassandra_inmem
from ceilometer.storage.cassandra import utils as cassandra_utils

LOG = log.getLogger(__name__)

_last_cql_timestamp = 0


def _build_cql_timestamp():
    global _last_cql_timestamp
    # Hack to work around Cassandra-6106
    t = time.time()
    t = int(t * 1E6)
    if t < _last_cql_timestamp:
        t = _last_cql_timestamp + 1
    _last_cql_timestamp = t
    LOG.info("Generated timestamp: %s", t)
    return t


def dump(data):
    # XXX: Is the bson thing needed?
    return json.dumps(data)  # , default=bson.json_util.default)


class CassandraKeyspace():
    """Models a keyspace in C*."""
    def __init__(self, name):
        self.name = name
        self.tables = []
        self.escaped_name = cassandra_utils.escape_identifier(name)


class CassandraColumn():
    """Models a column in C*."""
    def __init__(self, table, name, column_type):
        self.table = table
        self.name = name
        self.type = column_type
        self.escaped_name = cassandra_utils.escape_column(name)

    def cast_to_db(self, value):
        if self.type == 'uuid':
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
        return value

    def _build_empty(self):
        """Creates an empty value of self.type.

        Used by the in-memory mock.
        """
        if self.type.startswith('set<'):
            return set()
        if self.type.startswith('map<'):
            return {}
        return None


class CassandraIndex():
    """Models an index in C*."""
    def __init__(self, table, columns):
        self.table = table
        self.columns = columns
        column_names = [c.name for c in columns]
        self.name = '_'.join([self.table.name] + column_names)
        self.escaped_name = cassandra_utils.escape_identifier(self.name)


class CassandraIndexTable():
    """Models a table that is a secondary index."""
    def __init__(self, data_table, index_table, mapper, on_insert):
        self.data_table = data_table
        self.index_table = index_table
        self.mapper = mapper
        self.on_insert = on_insert

    def _after_insert(self, connection, data, ttl):
        mapped = self.mapper(data, ttl)
        if mapped is None:
            return
        mapped_ttl = mapped.pop('_ttl', None)
        return connection.insert(self.index_table,
                                 mapped,
                                 ttl=mapped_ttl)


class CassandraTable():
    """Models a table in C*."""
    def __init__(self, keyspace, name, pks, columns,
                 partition_key_length=1, indexes=[],
                 clustering_key=None):
        self.keyspace = keyspace
        self.name = name
        self.pks = pks
        self.escaped_name = cassandra_utils.escape_table(name)
        self.partition_key_length = partition_key_length

        self.column_map = {}
        self.columns = []
        for column_name, column_type in columns.iteritems():
            column = CassandraColumn(self, column_name, column_type)
            self.column_map[column_name] = column
            self.columns.append(column)

        self.secondary_indexes = {}
        for ix in indexes:
            index_columns = [self.get_column(c) for c in ix]
            i = CassandraIndex(self, index_columns)
            self.secondary_indexes[i.name] = i

        if clustering_key is None:
            self.clustering_key = []
            for k in pks[partition_key_length:]:
                self.clustering_key.append({'field': k})
        else:
            self.clustering_key = clustering_key

        self.secondary_indexes = {}
        for ix in indexes:
            index_columns = [self.get_column(c) for c in ix]
            i = CassandraIndex(self, index_columns)
            self.secondary_indexes[i.name] = i

        self.index_tables = []

        keyspace.tables.append(self)

    def get_column(self, column_name):
        column_def = self.column_map[column_name]
        if not column_def:
            raise Exception("Unknown column: " + column_def)
        return column_def

    def add_index(self, table, mapper, on_insert=False):
        self.index_tables.append(CassandraIndexTable(self,
                                                     table,
                                                     mapper,
                                                     on_insert))


def _matches_complex_predicate(predicate, actual):
    """Evaluates a complex predicate against a value."""
    for op, expected in predicate.iteritems():
        if op == '<':
            if not expected < actual:
                return False
        elif op == '<=':
            if not expected <= actual:
                return False
        elif op == '>=':
            if not expected >= actual:
                return False
        elif op == '>':
            if not expected > actual:
                return False
        else:
            raise Exception('Unknown operator in predicate: ' + op)
    return True


def _matches_simple_predicate(expected, actual):
    """Evaluates a simple predicate against a value."""
    return expected == actual


class CassandraQuery():
    """A nicer API around executing a C* query, with postfiltering."""
    def __init__(self, keyspace, table):
        self.keyspace = keyspace
        self.table = table
        self.predicates = []
        self.parameters = []
        self.postfilters = {}
        self.cql = None

    def _add_pk_filter(self, field, op, expected):
        cql_op = op

        column = self.table.get_column(field)

        if column.type == 'timeuuid':
            db_value = expected
            self.parameters.append(db_value)
            cql_col = column.escaped_name
            if op == '<':
                self.predicates.append('(%s < maxTimeUuid(?))' % cql_col)
            elif op == '<=':
                # TODO(justinsb): This may actually be <, not <=,
                # because maxTimeUuid is FUBAR:
                # http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/timeuuid_functions_r.html
                # Add one microsecond if so??
                # (though surely this bug won't stand..)
                self.predicates.append('(%s <= maxTimeUuid(?))' % cql_col)
            elif op == '>=':
                # XXX: Same problem as <= ?
                self.predicates.append('(%s >= minTimeUuid(?))' % cql_col)
            elif op == '>':
                self.predicates.append('(%s > minTimeUuid(?))' % cql_col)
            elif op == '=':
                LOG.warning("Suspicious comparison: equality in timeuuid")
                self.predicates.append('(%s >= minTimeUuid(?))' % cql_col)
                self.predicates.append('(%s <= maxTimeUuid(?))' % cql_col)

                # We need an extra parameter
                self.parameters.append(db_value)
            else:
                raise Exception('Unknown operator in predicate: ' + op)

            # TODO(justinsb): Do we need the post-filter?
            # Probably not, unless we do shifting to work-around bugs
            # self._add_postfilter(field, { op: expected })
        else:
            self.predicates.append('(%s %s ?)' % (column.escaped_name, cql_op))
            self.parameters.append(column.cast_to_db(expected))

    def _add_postfilter(self, field, predicate):
        column = self.table.get_column(field)

        is_complex = isinstance(predicate, dict)
        if is_complex:
            db_predicate = {}
            for op, arg in predicate.iteritems():
                db_predicate[op] = column.cast_to_db(arg)
            postfilter = functools.partial(_matches_complex_predicate,
                                           db_predicate)
        else:
            db_predicate = column.cast_to_db(predicate)
            postfilter = functools.partial(_matches_simple_predicate,
                                           db_predicate)
        if field in self.postfilters:
            raise Exception("Double postfilter found")
        self.postfilters[field] = postfilter

    def build_query(self,
                    filt=None,
                    limit=None,
                    order=None,
                    extra_columns=None):
        self.predicates = []
        self.parameters = []
        self.postfilters = {}
        self.cql = None

        filt = filt or {}

        for field, predicate in filt.items():
            if predicate is None:
                continue

            # If a dict: keys which are operators; values are arguments
            # Typically used for timestamp values
            is_complex = isinstance(predicate, dict)

            if field in self.table.pks:
                if is_complex:
                    for op, expected in predicate.iteritems():
                        self._add_pk_filter(field, op, expected)
                else:
                    self._add_pk_filter(field, '=', predicate)
            else:
                self._add_postfilter(field, predicate)

        if extra_columns:
            # Workaround for CQL design flaw: can't do SELECT *, writetime(X)
            columns = [col.escaped_name for col in self.table.columns]
            columns = columns + extra_columns
            cql = 'SELECT ' + ','.join(columns)
        else:
            cql = 'SELECT *'
        cql = cql + ' FROM ' + (self.table.escaped_name)

        if self.predicates:
            # Maybe we should throw if there are no predicates(?)
            cql = cql + ' WHERE ' + ' AND '.join(self.predicates)

        if order is not None:
            order_bys = []
            for o in order:
                key = o['field']
                reverse = o.get('reverse', False)
                asc_or_desc = 'DESC' if reverse else 'ASC'
                order_bys.append(key + ' ' + asc_or_desc)
            cql = cql + ' ORDER BY ' + ','.join(order_bys)

        if limit is not None:
            cql = cql + ' LIMIT ' + str(limit)

        self.cql = cql

    def execute(self, connection):
        rows = connection.execute(self.keyspace, self.cql, self.parameters)
        if not self.postfilters:
            return rows

        # XXX: Generator?
        matches = []

        for row in rows:
            match = True
            for k, postfilter in self.postfilters.iteritems():
                v = getattr(row, k)
                if not postfilter(v):
                    match = False
                    break
            if match:
                matches.append(row)

        return matches


class CassandraConnection():
    """A connection to a C* cluster."""
    def __init__(self, url):
        """Cassandra Connection Initialization."""
        self.url = url

        self._sessions = {}

        self._use_client_timestamp = False

        self._prepared = {}

        opts = self._parse_connection_url(url)
        self.keyspace_prefix = opts['keyspace_prefix'] or ''
        self._cluster = None

    def close(self):
        if self._cluster:
            self._cluster.shutdown()
            self._cluster = None
            self._prepared = {}
            self._sessions = {}

    def _get_session(self, keyspace):
        """Returns the session bound to the specified keyspace.

        We have one session per keyspace.
        """
        keyspace_name = None
        if keyspace:
            keyspace_name = self.keyspace_prefix + keyspace.name
        session = self._sessions.get(keyspace_name)
        if not session:
            session = self._get_cluster().connect(keyspace_name)
            self._sessions[keyspace_name] = session
        return session

    def _close_session(self, keyspace):
        keyspace_name = None
        if keyspace:
            keyspace_name = self.keyspace_prefix + keyspace.name
        session = self._sessions.get(keyspace_name)
        if session:
            session.shutdown()
            self._sessions[keyspace_name] = None

    def execute(self, keyspace, cql,
                parameters=None,
                timeout=cassandra.cluster._NOT_SET):
        session = self._get_session(keyspace)

        LOG.debug(_("Executing CQL: %(cql)s %(parameters)s") %
                  {'cql': cql, 'parameters': parameters})

        # XXX: The syntax for non-prepared set updates is different?
        prepare = True
        consistency_level = cassandra.ConsistencyLevel.QUORUM

        if prepare:
            ps = self._prepared.get(cql)
            if ps is None:
                ps = session.prepare(cql)
                # TODO(justinsb): LRU?
                self._prepared[cql] = ps
            bound = ps.bind(parameters)
            bound.consistency_level = consistency_level
            return session.execute(bound, timeout=timeout)
        else:
            # Prepare and non-prepared are inconsistent.
            cql = cql.replace('?', '%s')
            s = cassandra.query.SimpleStatement(
                cql,
                consistency_level=consistency_level)
            return session.execute(s, parameters, timeout=timeout)

    def drop_tables(self, keyspace):
        for table in keyspace.tables:
            self.drop_table(keyspace, table, if_exists=True)

    def drop_table(self, keyspace, table, if_exists=False):
        cql = 'DROP TABLE '
        if if_exists:
            cql = cql + 'IF EXISTS '
        cql = cql + table.escaped_name
        self.execute(keyspace, cql)

    def truncate_table(self, table):
        keyspace = table.keyspace
        cql = 'TRUNCATE %s' % (table.escaped_name)
        self.execute(keyspace, cql)

    def truncate_tables(self, keyspace):
        for table in keyspace.tables:
            self.truncate_table(table)

    def drop_keyspace(self, keyspace, if_exists=False, timeout=None):
        keyspace_name = self.keyspace_prefix + keyspace.name
        cql = 'DROP KEYSPACE '
        if if_exists:
            cql = cql + 'IF EXISTS '
        cql = cql + keyspace_name
        self.execute(None, cql, timeout=timeout)
        self._close_session(keyspace)

        # Hack to avoid memory leak
        # TODO(justinsb): Do we need this??
        self._get_cluster().submit_schema_refresh()

    @staticmethod
    def _parse_connection_url(url):
        """Parse connection parameters from a database url."""
        opts = {}
        result = netutils.urlsplit(url)
        opts['keyspace_prefix'] = urlparse.parse_qs(
            result.query).get('keyspace_prefix', [None])[0]
        opts['dbtype'] = result.scheme
        if ':' in result.netloc:
            opts['host'], port = result.netloc.split(':')
        else:
            opts['host'] = result.netloc
            port = 9042
        opts['port'] = port and int(port) or 9042
        return opts

    @staticmethod
    def _is_mock(url):
        opts = CassandraConnection._parse_connection_url(url)

        if opts['host'] == '__test__':
            url = os.environ.get('CEILOMETER_TEST_CASSANDRA_URL')
            if not url:
                return True
        return False

    def _get_cluster(self):
        if self._cluster:
            return self._cluster

        url = self.url
        opts = CassandraConnection._parse_connection_url(url)

        if opts['host'] == '__test__':
            url = os.environ.get('CEILOMETER_TEST_CASSANDRA_URL')
            if url:
                # Reparse URL, but from the env variable now
                opts = self._parse_connection_url(url)
                self._cluster = self._connect_cluster(opts)
            else:
                # This should be unreachable
                raise Exception("Should have constructed a mock")
        else:
            self._cluster = self._connect_cluster(opts)

        return self._cluster

    @staticmethod
    def _connect_cluster(conf):
        """Return a Cassandra Cluster instance, managing the cluster

        .. note::

          The tests use a subclass to override this and return an
          in-memory connection pool.
        """
        # TODO(justinsb): Multiple hosts
        hosts = [conf['host']]
        port = conf['port']
        LOG.debug(_('connecting to Cassandra on %(hosts)s with %(port)s') % (
                  {'hosts': hosts, 'port': port}))
        cluster = cassandra.cluster.Cluster(hosts, port)
        cluster.connection_class = eventletreactor.EventletConnection
        return cluster

    def insert(self, table, data=None, ttl=None):
        """Insert dict into specified keyspace & table."""
        keyspace = table.keyspace

        fields = {}
        for k, v in data.items():
            fields[k] = v

        columns = []
        values = []
        parameters = []

        for k, v in fields.items():
            column = table.get_column(k)

            columns.append(column.escaped_name)
            values.append('?')
            parameters.append(column.cast_to_db(v))

        cql = ('INSERT INTO %s (%s) VALUES (%s)'
               % (table.escaped_name,
                  ','.join(columns), ','.join(values)))

        using = []
        if self._use_client_timestamp:
            using.append('TIMESTAMP ?')
            parameters.append(_build_cql_timestamp())

        if ttl:
            using.append('TTL ?')
            parameters.append(ttl)

        if using:
            cql = cql + ' USING ' + ' AND '.join(using)

        ret = self.execute(keyspace, cql, parameters)

        for index_table in table.index_tables:
            if not index_table.on_insert:
                continue
            index_table._after_insert(self, data, ttl)
        return ret

    def update(self, table, data, merge_columns=[]):
        """Update dict into specified keyspace & table.

        :param data: dict to be serialized
        :param kwargs: additional args
        """
        keyspace = table.keyspace

        data = data or {}

        fields = {}
        for k, v in data.items():
            fields[k] = v

        column_updates = []
        parameters = []
        where_clauses = []

        for k, v in fields.items():
            if k in table.pks:
                continue
            column = table.get_column(k)
            if k in merge_columns:
                column_updates.append('%s=%s+?'
                                      % (column.escaped_name,
                                         column.escaped_name))
            else:
                column_updates.append('%s=?' % (column.escaped_name))
            parameters.append(column.cast_to_db(v))

        for pk in table.pks:
            if pk not in fields:
                raise Exception("Must specify PK column: %s" % (pk))
            v = fields[pk]
            column = table.get_column(pk)
            where_clause = "(%s=?)" % (column.escaped_name)
            where_clauses.append(where_clause)
            parameters.append(column.cast_to_db(v))

        cql = 'UPDATE ' + table.escaped_name

        if self._use_client_timestamp:
            cql = cql + ' USING TIMESTAMP ?'
            parameters.insert(0, _build_cql_timestamp())

        cql += 'SET %s WHERE %s' % (','.join(column_updates),
                                    ' AND '.join(where_clauses))

        ret = self.execute(keyspace, cql, parameters)
        return ret

    def select(self,
               table,
               filt=None,
               limit=None,
               order=None,
               extra_columns=None):
        """Selects values from keyspace/table matching the filter."""
        keyspace = table.keyspace
        query = CassandraQuery(keyspace, table)
        query.build_query(filt=filt,
                          limit=limit,
                          order=order,
                          extra_columns=extra_columns)

        return query.execute(self)

    def delete(self, keyspace, table, filt=None):
        """Deletes values from keyspace/table matching the filter."""
        filt = filt or {}

        predicates = []
        parameters = []

        for k, v in filt.items():
            if v is None:
                continue
            column = table.get_column(k)
            if k in table.pks:
                predicates.append('(%s=?)' % column.escaped_name)
                parameters.append(column.cast_to_db(v))
            else:
                raise Exception("Not PK")

        cql = 'DELETE FROM ' + (table.escaped_name)

        if self._use_client_timestamp:
            cql = cql + ' USING TIMESTAMP ?'
            parameters.insert(0, _build_cql_timestamp())

        if predicates:
            # TODO(justinsb): Block if no predicates?
            cql = cql + ' WHERE ' + ' AND '.join(predicates)

        self.execute(keyspace, cql, parameters)


class CassandraConnectionLease(object):
    """Tracks a CassandraConnection borrowed from a CassandraConnectionPool."""
    def __init__(self, pool, connection, url):
        self._pool = pool
        self.connection = connection
        self.url = url

    def __enter__(self):
        # TODO(justinsb): Should we return self.connection here?
        # It would hide the least, but that doesn't appear to be idiomatic
        #     return  self.connection
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.return_lease(self)


# TODO(justinsb): Do we need the pool?
#  Bump the max size to 1 billion to find out
class CassandraConnectionPool(object):
    """Connection pool for C*."""
    def __init__(self, capacity=16):
        self.capacity = capacity
        self._pool = {}
        self._last_used = {}
        self._in_use = {}

    @staticmethod
    def get_default():
        return DEFAULT_CONNECTION_POOL

    def get(self, url):
        """Return a CassandraConnectionLease, with LRU caching."""
        connection = self._pool.get(url)
        call_cleanup = False
        if not connection:
            LOG.debug(_("Connection pool making new connection to %s") % url)
            if CassandraConnection._is_mock(url):
                # This is a in-memory usage for unit tests
                opts = CassandraConnection._parse_connection_url(url)
                keyspace_prefix = opts['keyspace_prefix'] or ''
                connection = cassandra_inmem.MemConnection.get(keyspace_prefix)
            else:
                connection = CassandraConnection(url)
            self._pool[url] = connection
            call_cleanup = True

        lease = CassandraConnectionLease(self, connection, url)
        self._lock(lease)
        self._last_used[url] = time.time()

        if call_cleanup:
            self._maybe_cleanup()

        return lease

    def _retention_score(self, url):
        in_use = self._in_use.get(url, 0)
        if in_use:
            return float("inf")
        last_used = self._last_used.get(url, 0)
        return last_used

    def _maybe_cleanup(self):
        while len(self._pool) > self.capacity:
            min_key = min(self._pool.keys(), key=self._retention_score)
            if self._in_use.get(min_key):
                # Performance will take a dive if this happens, not only
                # because we're doing the cleanup logic all the time,
                # but also because we're thrashing through C* connections
                LOG.warning(_("Cannot evict from connection pool, "
                              "all connections are in use"))
                return
            connection = self._pool.pop(min_key)
            self._last_used.pop(min_key)
            self._in_use.pop(min_key)
            LOG.debug(_("Connection pool closing connection to %s") %
                      connection.url)
            connection.close()

    def return_lease(self, lease):
        count = self._in_use.get(lease.url, 0)
        count = count - 1
        self._in_use[lease.url] = count
        if count == 0 and len(self._pool) > self.capacity:
            self._maybe_cleanup()

    def _lock(self, lease):
        count = self._in_use.get(lease.url, 0)
        self._in_use[lease.url] = count + 1

DEFAULT_CONNECTION_POOL = CassandraConnectionPool(capacity=16)
