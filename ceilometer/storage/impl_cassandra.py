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
import datetime
import json
import operator

from oslo.config import cfg
from oslo.utils import timeutils

import ceilometer
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.publisher import utils as publisher_utils
from ceilometer.storage import base
from ceilometer.storage.cassandra import buckets
from ceilometer.storage.cassandra import connection
from ceilometer.storage.cassandra import migration
from ceilometer.storage.cassandra import utils as cassandra_utils
from ceilometer.storage import models
from ceilometer import utils

cfg.CONF.import_opt('time_to_live', 'ceilometer.storage',
                    group="database")

LOG = log.getLogger(__name__)

EPOCH_Y2K = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, None)

_GENESIS = datetime.datetime(year=datetime.MINYEAR, month=1, day=1)
_APOCALYPSE = datetime.datetime(year=datetime.MAXYEAR, month=12, day=31,
                                hour=23, minute=59, second=59)

AVAILABLE_CAPABILITIES = {
    'meters': {'query': {'simple': True,
                         'metadata': True}},
    'resources': {'query': {'simple': True,
                            'metadata': True}},
    'samples': {'query': {'simple': True,
                          'metadata': True}},
    'statistics': {'query': {'simple': True,
                             'metadata': True},
                   'aggregation': {'standard': True}},
}

AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': False},
}

KEYSPACE = connection.CassandraKeyspace("metrics")

# The resource table holds a row per resource.
# We expect this to be quite a small row,
# and likely many rows (compared to samples)
RESOURCE_TABLE = connection.CassandraTable(
    KEYSPACE,
    'resource',
    ['resource_id'],
    {
        'resource_id': 'text',
        'project_id': 'text',
        'user_id': 'text',
        'source': 'text',
        'metadata': 'text',
        # TODO(justinsb): meters is now the wrong name; we put
        # metadata here that we would otherwise have to put on the
        # sample; not just the meter info
        'meters': 'map<text, text>',
        'first_sample_timestamp': 'timestamp',
        # TODO(justinsb): Replace metadata_timestamp with column
        # timestamp on metadata
        'metadata_timestamp': 'timestamp'
    }
)

# We store 'statistics', although the primary purpose
# is to know which date values have data for a given meter,
# to avoid a range lookup in samples
METER_STATS_TABLE = connection.CassandraTable(
    KEYSPACE,
    'meter_stats',
    ['resource_id', 'meter_id', 'timebucket'],
    {
        'resource_id': 'text',
        'meter_id': 'text',
        'timebucket': 'int',
        'statistics': 'map<text, double>'
    },
    # Partition on resource_id, meter_id
    partition_key_length=2,
    # Optimize for retrieving the latest data
    clustering_key=[
        {'field': 'timebucket', 'reverse': True}
    ]
)

# This is the main table, which stores the samples
# (Ceilometer calls samples 'meters').
# This table will likely be the biggest, at least by "item count"
# We use wide rows here:
#   The row key is the meter_id (with a sharding key)
#   The column key is the timestamp & message_id
#   The column value is just the value
#
# Note that we have effectively denormalized most of the data into meter
#
# timebucket is a sharding key, and just stops rows from becoming huge.
#
# TODO(justinsb): Should we store the message_signature as part of the key?
# We store the resource_id as well, even though the meter_id should
# encode it, in case of collisions on meter_id.  It would be bad to show
# someone else's data, however unlikely!
SAMPLE_TABLE = connection.CassandraTable(
    KEYSPACE,
    "sample",
    ['resource_id', 'meter_id', 'timebucket', 'ts', 'message_id'],
    {
        'resource_id': 'text',
        'meter_id': 'text',
        'timebucket': 'int',
        # TODO(justinsb): milliseconds since start of bucket?
        'ts': 'timestamp',
        'message_id': 'text',
        'message_signature': 'text',
        'volume': 'double',
        'resource_metadata': 'text'
    },
    # Partition on resource_id, meter_id and timebucket
    partition_key_length=3,
    # Optimize for retrieving the latest data
    clustering_key=[
        {'field': 'ts', 'reverse': True},
        {'field': 'message_id'}
    ]
)

SAMPLE_TIMEBUCKET = buckets.Timebucket(24 * 60 * 60, EPOCH_Y2K)


# SAMPLES_RECENT_INDEX is an index for recent samples
# (rows have a TTL, so expire fairly soon)
SAMPLES_RECENT_INDEX = connection.CassandraTable(
    KEYSPACE,
    "sample_ix_recent",
    ['timebucket', 'shard'],
    {
        'timebucket': 'int',
        'shard': 'int',
        'meter_ids': 'set<text>'
    }
)

SAMPLES_RECENT_TIMEBUCKET = buckets.Timebucket(300, EPOCH_Y2K)
SAMPLES_RECENT_MAXAGE = 2 * 60 * 60
SAMPLES_RECENT_TTL_GRACEPERIOD = 2 * 60 * 60
SAMPLES_RECENT_SHARD = buckets.HashShard(32)


def map_to_samples_recent(sample, sample_ttl):
    ts = sample['ts']
    meter_id = sample['meter_id']
    # TODO(justinsb): Only if not too old?  Should we base the ttl on ts?
    # TODO(justinsb): Micro-cache to avoid inserts for hot meters
    mapped = {}
    mapped['meter_ids'] = [meter_id]
    mapped['shard'] = SAMPLES_RECENT_SHARD.to_shard(meter_id)
    mapped['timebucket'] = SAMPLES_RECENT_TIMEBUCKET.to_bucket(ts)

    mapped['_ttl'] = SAMPLES_RECENT_MAXAGE + SAMPLES_RECENT_TTL_GRACEPERIOD
    return mapped


SAMPLE_TABLE.add_index(SAMPLES_RECENT_INDEX,
                       map_to_samples_recent,
                       on_insert=True)


# SAMPLE_BY_MESSAGEID_TABLE stores samples for retrieval by message_id
# We have to be able to retrieve data by message_id.
# The only viable approach seems to be to store the data twice.
# TODO(justinsb): Should we use wide rows using a prefix of the message id?
SAMPLE_BY_MESSAGEID_TABLE = connection.CassandraTable(
    KEYSPACE,
    "sample_by_messageid",
    ['message_id', 'resource_id', 'meter_id', 'ts'],
    {
        'message_id': 'text',
        'message_signature': 'text',
        'resource_id': 'text',
        'meter_id': 'text',
        'ts': 'timestamp',
        'volume': 'double',
        'resource_metadata': 'text'
    }
)


def map_to_sample_by_messageid(sample, sample_ttl):
    mapped = {}
    mapped['resource_id'] = sample['resource_id']
    mapped['meter_id'] = sample['meter_id']
    mapped['ts'] = sample['ts']
    mapped['message_id'] = sample['message_id']
    mapped['message_signature'] = sample['message_signature']
    mapped['volume'] = sample['volume']
    if 'resource_metadata' in sample:
        mapped['resource_metadata'] = sample['resource_metadata']

    mapped['_ttl'] = sample_ttl
    return mapped


SAMPLE_TABLE.add_index(SAMPLE_BY_MESSAGEID_TABLE,
                       map_to_sample_by_messageid,
                       on_insert=True)


def _is_at_range_edge(range, value):
    """Check if the value is at a transition point of the range.

    i.e. checks if (value + 1) or (value - 1) is not in range.
    Only meaningful if value is in range.
    """
    for op, v in range.iteritems():
        if op == '>=':
            if v == value:
                return True
        elif op == '<=':
            if v == value:
                return True
        else:
            raise Exception("Unexpected operator: %s" % (op))


def predicate_matches(predicate, data):
    for k, expected_v in predicate.iteritems():
        actual_v = data.get(k)
        if actual_v != expected_v:
            return False
    return True


class Connection(base.Connection):
    """Storage driver for the Cassandra database."""

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

        # TODO(justinsb): Should we have some padding here?
        self.ttl = cfg.CONF.database.time_to_live
        if self.ttl <= 0:
            self.ttl = None

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

    def _build_meter_id(self, meter):
        # TODO(justinsb): Do we want to use a secret here?
        secret = cfg.CONF.publisher.metering_secret
        sig = publisher_utils.compute_signature(meter, secret)
        return sig

    def clear_expired_metering_data(self, ttl):
        """Clear expired data from the backend storage system.

        Clearing occurs according to the time-to-live.
        :param ttl: Number of seconds to keep records for.
        """
        # TODO(justinsb): Implement this (despite the use of TTL above!)
        # We need to delete old non-sample data
        # (TTL might change)
        pass

    def record_metering_data(self, data):
        """Write the data to the backend storage system.

        :param data: a dictionary such as returned by
          ceilometer.meter.meter_message_from_counter
        """

        # The update rules for resources / meters are complex:
        # resource_id is the primary key
        # We always update: project_id, user_id, source, metadata
        #  (technically, only if a later sample, but other data stores
        #   only implement that for metadata)
        # Update only on new resource: metadata
        # Meters are a set of tuples, comprising
        #  (counter_name, counter_type, counter_unit);
        #  we record them all
        # first_sample_timestamp & last_sample_timestamp should be recorded
        # first_sample_timestamp is cheap; we normally only write it once
        # last_sample_timestamp is expensive; we would have to write it on
        # every sample.  So we don't update it at all; the bounds don't have
        # to be tight...

        LOG.debug(_("Record meter data: %s") % data)

        user_id = data['user_id']
        resource_id = data['resource_id']
        project_id = data['project_id']
        timestamp = data['timestamp']
        source = data['source']

        meter = {}
        meter['resource_id'] = resource_id
        meter['meter_data'] = meter_map = {}
        meter_map['name'] = data['counter_name']
        meter_map['project_id'] = project_id
        meter_map['source'] = source
        meter_map['type'] = data['counter_type']
        meter_map['unit'] = data['counter_unit']
        meter_map['user_id'] = user_id

        # TODO(justinsb): Build directly?
        # TODO(justinsb): Build with only the values we want to uniquify?
        meter_id = self._build_meter_id(meter)
        meter['meter_id'] = meter_id

        with self._lease_connection() as c:
            conn = c.connection
            # TODO(justinsb): Micro-cache this lookup?
            # Check if the resource exists
            # TODO(justinsb): This must be a consistent read for metadata
            # update to be correct.
            # TODO(justinsb): Store versioned metadata?
            resource_rows = conn.select(RESOURCE_TABLE,
                                        {'resource_id': resource_id},
                                        limit=1)

            update_resource = False
            update_resource_metadata = False
            update_first_sample_timestamp = False

            # Resource (probably) doesn't exist yet; insert it
            resource = {}
            if len(resource_rows) == 0:
                update_resource = True
                update_resource_metadata = True
                update_first_sample_timestamp = True
            else:
                # Bizarrely, we change project, user and source on each write
                # But we only record the _first_ resource metadata
                resource_row = resource_rows[0]
                if resource_row.project_id != project_id:
                    update_resource = True
                elif resource_row.user_id != user_id:
                    update_resource = True
                elif resource_row.source != source:
                    update_resource = True

                if (resource_row.first_sample_timestamp is None
                        or (resource_row.first_sample_timestamp
                            > data['timestamp'])):
                    # TODO(justinsb): We probably shouldn't update the
                    # source,project,user
                    update_resource = True
                    update_first_sample_timestamp = True
                if resource_row.metadata_timestamp is None:
                    update_resource = True
                    update_resource_metadata = True
                elif resource_row.metadata_timestamp <= data['timestamp']:
                    metadata = data.get('resource_metadata')
                    metadata_json = json.dumps(metadata) if metadata else ''
                    if metadata_json != resource_row.metadata:
                        update_resource = True
                        update_resource_metadata = True

            if update_resource:
                resource['resource_id'] = resource_id
                if update_resource:
                    resource['project_id'] = project_id
                    resource['user_id'] = user_id
                    resource['source'] = source

                if update_first_sample_timestamp:
                    resource['first_sample_timestamp'] = data['timestamp']
                if update_resource_metadata:
                    metadata = data.get('resource_metadata')
                    if metadata:
                        resource['metadata'] = json.dumps(metadata)
                    else:
                        resource['metadata'] = ''
                    resource['metadata_timestamp'] = data['timestamp']
                conn.insert(RESOURCE_TABLE, resource)

            # TODO(justinsb): Micro-cache this lookup?
            sample_timebucket = SAMPLE_TIMEBUCKET.to_bucket(timestamp)
            meter_stats_rows = conn.select(METER_STATS_TABLE,
                                           {
                                               'resource_id': resource_id,
                                               'meter_id': meter_id,
                                               'timebucket': sample_timebucket
                                           },
                                           limit=1)
            if len(meter_stats_rows) == 0:
                # Check if the meter already exists
                meter_stats_rows = conn.select(METER_STATS_TABLE,
                                               {
                                                   'resource_id': resource_id,
                                                   'meter_id': meter_id
                                               },
                                               limit=1)
                if len(meter_stats_rows) == 0:
                    # Meter (probably) doesn't exist yet
                    resource = {}
                    resource['resource_id'] = resource_id
                    resource['meters'] = {meter_id: json.dumps(meter_map)}

                    # And insert the meter now
                    conn.update(RESOURCE_TABLE,
                                resource,
                                merge_columns=['meters'])

                # Now that the meter & resource exist, update the stats
                meter_stats = {}
                meter_stats['resource_id'] = resource_id
                meter_stats['meter_id'] = meter_id
                meter_stats['timebucket'] = sample_timebucket
                meter_stats['statistics'] = {}
                conn.insert(METER_STATS_TABLE, meter_stats)
            else:
                meter_stats_row = meter_stats_rows[0]
                if meter_stats_row.statistics:
                    # TODO(justinsb): Need to find a consistency-safe way
                    #  to do this (maybe set statistics to the max sample
                    #  write timestamp?)
                    update_meter_stats = {}
                    update_meter_stats.resource_id = resource_id
                    update_meter_stats.meter_id = meter_id
                    update_meter_stats.timebucket = sample_timebucket
                    update_meter_stats.statistics = {}

                    # Note: we _don't_ specify statistics as a merge column,
                    # so it will be overwritten
                    conn.update(METER_STATS_TABLE, update_meter_stats)

            sample = {}
            sample['resource_id'] = resource_id
            sample['meter_id'] = meter_id
            sample['timebucket'] = sample_timebucket
            sample['message_id'] = data['message_id']
            sample['message_signature'] = data['message_signature']
            sample['ts'] = timestamp
            sample['volume'] = data.get('counter_volume')

            # TODO(justinsb): Use Cassandra write timestamp for recorded_at?
            # (not recommended, but is recorded_at really that important?)
            # sample['recorded_at'] = timeutils.utcnow()

            # TODO(justinsb): only if different / do delta encoding?
            # This is really expensive to store.
            # Of course, this is also tricky because of eventual consistency
            # We could do the same trick we pull on meter_id
            # (Redefine meter_id to include the resource)
            # We would need to rename!
            if True and data.get('resource_metadata'):
                resource_metadata = data.get('resource_metadata')
                sample['resource_metadata'] = json.dumps(resource_metadata)

            conn.insert(SAMPLE_TABLE, sample, ttl=self.ttl)

    def _find_last_ts(self, conn, resource):
        """Finds the last sample timestamp for the resource.

        This is quite expensive - we don't want to slow down writing, but
        this means that we have to do a fairly complicated scan of the data
        instead.  Our partition keys help though.
        """

        resource_id = resource.resource_id
        # We have to loop over all meters, because we need the meter_id
        # for the samples partition keys.  But we don't actually expect that
        # many meters.
        max_timebucket = None
        meter_ids = []
        for meter_id in resource.meters.iterkeys():
            stats = conn.select(METER_STATS_TABLE,
                                {
                                    'resource_id': resource_id,
                                    'meter_id': meter_id
                                },
                                order=[
                                    {'field': 'timebucket', 'reverse': True}
                                ],
                                limit=1)
            if stats:
                stat = stats[0]
                meter_id = stat.meter_id
                timebucket = stat.timebucket
                if max_timebucket is None or timebucket > max_timebucket:
                    meter_ids = []
                    max_timebucket = timebucket
                elif timebucket < max_timebucket:
                    continue
                meter_ids.append(meter_id)

        if not meter_ids:
            return None

        max_ts = None
        for meter_id in meter_ids:
            sample_rows = conn.select(SAMPLE_TABLE,
                                      {
                                          'resource_id': resource_id,
                                          'meter_id': meter_id,
                                          'timebucket': max_timebucket
                                      },
                                      order=[
                                          {'field': 'ts', 'reverse': True}
                                      ],
                                      limit=1)

            if sample_rows:
                sample_row = sample_rows[0]
                ts = sample_row.ts
                if max_ts is None or max_ts < ts:
                    max_ts = ts

        return max_ts

    def get_resources(self, user=None, project=None, source=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None,
                      metaquery=None, resource=None, pagination=None):
        """Return an iterable of models.Resource instances

        :param user: Optional ID for user that owns the resource.
        :param project: Optional ID for project that owns the resource.
        :param source: Optional source filter.
        :param start_timestamp: Optional modified timestamp start range.
        :param start_timestamp_op: Optional start time operator, like ge, gt.
        :param end_timestamp: Optional modified timestamp end range.
        :param end_timestamp_op: Optional end time operator, like lt, le.
        :param metaquery: Optional dict with metadata to match on.
        :param resource: Optional resource filter.
        :param pagination: Optional pagination query.
        """
        if pagination:
            raise ceilometer.NotImplementedError('Pagination not implemented')

        if metaquery:
            raise ceilometer.NotImplementedError('metaquery not implemented')

        resource_q = {}
        if user:
            resource_q['user_id'] = user
        if project:
            resource_q['project_id'] = project
        if resource:
            resource_q['resource_id'] = resource
        if source:
            resource_q['source'] = source

        if start_timestamp or end_timestamp:
            raise ceilometer.NotImplementedError("timestamp query "
                                                 "not implemented")

        if metaquery:
            raise ceilometer.NotImplementedError("metaquery not implemented")

        LOG.debug(_("Executing query: %s") % (resource_q))

        resources = []
        with self._lease_connection() as c:
            for r in c.connection.select(RESOURCE_TABLE,
                                         resource_q):
                metadata = r.metadata
                if metadata:
                    metadata = json.loads(metadata)

                first_ts = r.first_sample_timestamp
                last_ts = self._find_last_ts(c.connection, r)

                resource = models.Resource(
                    resource_id=r.resource_id,
                    first_sample_timestamp=first_ts,
                    last_sample_timestamp=last_ts,
                    project_id=r.project_id,
                    source=r.source,
                    user_id=r.user_id,
                    metadata=metadata)
                resources.append(resource)

        # sort_keys = ['user_id', 'project_id', 'timestamp']
        # sort_keys = ['first_sample_timestamp' if k == 'timestamp' else k
        #               for k in sort_keys]
        # # TODO(justinsb): Implement compound comparator
        # for sort_key in sort_keys:
        #     resources.sort(key=lambda x: getattr(x, sort_key))
        return resources

    def get_meters(self, user=None, project=None, resource=None, source=None,
                   metaquery=None, pagination=None):
        """Return an iterable of models.Meter instances

        :param user: Optional ID for user that owns the resource.
        :param project: Optional ID for project that owns the resource.
        :param resource: Optional resource filter.
        :param source: Optional source filter.
        :param metaquery: Optional dict with metadata to match on.
        :param pagination: Optional pagination query.
        """

        if pagination:
            raise ceilometer.NotImplementedError('Pagination not implemented')

        metaquery = metaquery or {}

        resource_q = {}
        filt = {}
        if user is not None:
            resource_q['user_id'] = user
        if project is not None:
            resource_q['project_id'] = project
        if resource is not None:
            resource_q['resource_id'] = resource
        if source is not None:
            resource_q['source'] = source

        if metaquery:
            raise ceilometer.NotImplementedError('Metaquery not implemented')
        # q.update(metaquery)

        LOG.debug(_("Querying for meters: %s") % resource_q)

        with self._lease_connection() as c:
            for r in c.connection.select(RESOURCE_TABLE,
                                         resource_q):
                meter_tuples = set()
                for meter_id, meter_json in r.meters.iteritems():
                    meter = json.loads(meter_json)
                    if not predicate_matches(filt, meter):
                        continue
                    meter_tuple = (meter['name'],
                                   meter['type'],
                                   meter.get('unit', ''))
                    if meter_tuple in meter_tuples:
                        continue
                    meter_tuples.add(meter_tuple)
                    yield models.Meter(
                        name=meter['name'],
                        type=meter['type'],
                        # Return empty string if 'counter_unit' is not valid,
                        # for backward compatibility.
                        unit=meter.get('unit', ''),
                        resource_id=r.resource_id,
                        project_id=r.project_id,
                        source=meter['source'],
                        user_id=meter['user_id']
                    )

    def get_samples(self, sample_filter, limit=None):
        """Return an iterable of models.Sample instances.

        :param sample_filter: Filter.
        :param limit: Maximum number of results to return.
        """

        LOG.debug(_("get_samples: %s") % vars(sample_filter))

        if limit == 0:
            return []

        sample_models = []

        def callback(resource, meter, sample):
            resource_metadata = None
            if sample.resource_metadata:
                resource_metadata = json.loads(sample.resource_metadata)
            elif resource.metadata:
                resource_metadata = json.loads(resource.metadata)

            recorded_at = sample.writetime_volume
            if recorded_at:
                recorded_at = datetime.datetime.utcfromtimestamp(recorded_at
                                                                 / 1000000.0)

            sample_model = models.Sample(
                source=meter['source'],
                counter_name=meter['name'],
                counter_type=meter['type'],
                # Return empty string if 'counter_unit' is not valid, for
                # backward compatibility.
                counter_unit=meter.get('unit', ''),
                counter_volume=sample.volume,
                user_id=meter['user_id'],
                project_id=resource.project_id,
                resource_id=resource.resource_id,
                timestamp=sample.ts,
                resource_metadata=resource_metadata,
                message_id=sample.message_id,
                message_signature=sample.message_signature,
                recorded_at=recorded_at)
            sample_models.append(sample_model)

        query = SampleQuery(self, sample_filter, limit, callback)
        query.query()

        return sorted(sample_models, key=operator.attrgetter('timestamp'),
                      reverse=True)

    def get_meter_statistics(self, sample_filter, period=None, groupby=None,
                             aggregate=None):
        """Return an iterable of models.Statistics instances.

        Items are containing meter statistics described by the query
        parameters. The filter must have a meter value set.
        """
        query = AggregateSampleQuery(self, sample_filter, period, groupby,
                                     aggregate)
        return query.query()


class SampleQuery(object):
    """Abstracts the strategies used to query samples.

    Querying samples is at the core of what we do, so we do lots of
    work to make it efficient.  Those tricks are encapsulated in this class.
    """

    def __init__(self, driver, sample_filter, limit=None, callback=None):
        self.driver = driver
        self.sample_filter = sample_filter
        self.limit = limit
        self.callback = callback

    def _on_sample_row(self, resource, meter, sample):
        if self.callback:
            self.callback(resource, meter, sample)

    def _query_by_messageid(self, connection):
        # TODO(justinsb): Move extra_columns to the table definition?
        for sample in connection.select(SAMPLE_BY_MESSAGEID_TABLE,
                                        self.sample_q,
                                        extra_columns=['writetime(volume)']):
            self.resource_q['resource_id'] = sample.resource_id
            for r in connection.select(RESOURCE_TABLE, self.resource_q):
                if not r.meters:
                    LOG.debug(_("Resource had no meters: %s"), r)
                    continue
                meter_id = sample.meter_id
                meter_json = r.meters.get(meter_id)
                if not meter_json:
                    LOG.debug(_("Resource did not have sample meter: %s"),
                              meter_id)
                    continue

                meter = json.loads(meter_json)
                # TODO(justinsb): Move to Cassandra utils
                # TODO(justinsb): Move to cassandra itself
                #  (i.e. have a METER_TABLE)
                if not predicate_matches(self.meter_q, meter):
                    continue
                self._on_sample_row(r, meter, sample)
        return

    def _query_with_recent_index(self, connection):
        ts_range = self.ts_range
        resource_q = self.resource_q
        meter_q = self.meter_q

        samples_recent_q = {}
        samples_recent_q['ts'] = SAMPLES_RECENT_TIMEBUCKET.map_range(ts_range)

        # Technically we should track (resource_id, meter_id, timebucket)
        # tuples, but it's good enough and much easier to track them separately
        resource_ids = set()
        meter_ids = set()
        recent_timebuckets = set()
        for r in connection.select(SAMPLES_RECENT_INDEX,
                                   samples_recent_q):
            resource_ids.add(r.resource_id)
            meter_ids |= r.meter_ids
            recent_timebuckets.add(r.timebucket)

        sample_timebuckets = set()
        for recent_timebucket in recent_timebuckets:
            sample_timebuckets.add(
                SAMPLES_RECENT_TIMEBUCKET.convert(recent_timebucket,
                                                  SAMPLE_TIMEBUCKET))

        resource_q['resource_id'] = {'in': resource_ids}

        for r in connection.select(RESOURCE_TABLE,
                                   resource_q):
            if not r.meters:
                LOG.debug(_("Resource had no meters: %s"), r)
                continue
            for meter_id, meter_json in r.meters.iteritems():
                if meter_id not in meter_ids:
                    continue

                meter = json.loads(meter_json)
                # TODO(justinsb): Move to Cassandra utils
                # TODO(justinsb): Move to cassandra itself (i.e. have a
                # METER_TABLE)
                if not predicate_matches(meter_q, meter):
                    continue

                self._on_sample_timebuckets(connection,
                                            r,
                                            meter_id,
                                            meter,
                                            sample_timebuckets)

    def _on_stats_table_row(self,
                            connection,
                            resource,
                            meter_id,
                            meter,
                            stats_table_row):
        timebucket = stats_table_row.timebucket

        self._on_sample_timebuckets(connection, resource, meter_id, meter,
                                    [timebucket])

    def _on_sample_timebuckets(self,
                               connection,
                               resource,
                               meter_id,
                               meter,
                               timebuckets):
        sample_q = self.sample_q

        # Note - we're editing sample_q in place
        # if we do parallel queries, we might have to clone
        sample_q['resource_id'] = resource.resource_id
        sample_q['meter_id'] = meter_id

        # TODO(justinsb): Move to our cassandra wrapper?
        if len(timebuckets) == 0:
            return
        elif len(timebuckets) == 1:
            sample_q['timebucket'] = timebuckets[0]
        else:
            sample_q['timebucket'] = {'in': timebuckets}

        for sample in connection.select(SAMPLE_TABLE,
                                        sample_q,
                                        extra_columns=['writetime(volume)']):
            self._on_sample_row(resource, meter, sample)

    def _query_default_strategy(self, connection):
        resource_q = self.resource_q
        meter_q = self.meter_q

        # The normal query procedure
        for r in connection.select(RESOURCE_TABLE,
                                   resource_q):
            if not r.meters:
                LOG.debug(_("Resource had no meters: %s"), r)
                continue
            for meter_id, meter_json in r.meters.iteritems():
                meter = json.loads(meter_json)
                # TODO(justinsb): Move to Cassandra utils
                # TODO(justinsb): Move to cassandra itself
                # (i.e. have a METER_TABLE)
                if not predicate_matches(meter_q, meter):
                    continue

                # TODO(justinsb): Skip if the date query is small
                # TODO(justinsb): And so unify with the recent query above
                stats_q = {}
                stats_q['resource_id'] = r.resource_id
                stats_q['meter_id'] = meter_id
                if self.sample_timebucket_range:
                    stats_q['timebucket'] = self.sample_timebucket_range
                for s in connection.select(METER_STATS_TABLE, stats_q):
                    self._on_stats_table_row(connection, r, meter_id, meter, s)

    def query(self):
        driver = self.driver
        sample_filter = self.sample_filter
        limit = self.limit

        if limit:
            raise ceilometer.NotImplementedError("Limit not implemented.")

        # First, we must identify the candidate meters
        resource_q = {}
        meter_q = {}
        sample_q = {}

        if sample_filter.user:
            meter_q['user_id'] = sample_filter.user
        if sample_filter.project:
            resource_q['project_id'] = sample_filter.project
        if sample_filter.meter:
            meter_q['name'] = sample_filter.meter
        #         elif require_meter:
        #             raise RuntimeError('Missing required meter specifier')

        if sample_filter.resource:
            resource_q['resource_id'] = sample_filter.resource
            sample_q['resource_id'] = sample_filter.resource
        if sample_filter.source:
            meter_q['source'] = sample_filter.source
        if sample_filter.message_id:
            sample_q['message_id'] = sample_filter.message_id

        ts_range = cassandra_utils.make_timestamp_range(
            sample_filter.start,
            sample_filter.end,
            sample_filter.start_timestamp_op,
            sample_filter.end_timestamp_op)
        #         if ts_range:
        #             q['timestamp'] = ts_range

        sample_timebucket_range = None

        if ts_range:
            sample_q['ts'] = ts_range
            sample_timebucket_range = SAMPLE_TIMEBUCKET.map_range(ts_range)
            sample_q['timebucket'] = sample_timebucket_range

        ts_range_is_recent = False
        if ts_range:
            lo = ts_range.get('>') or ts_range.get('>=')
            if lo:
                age = timeutils.delta_seconds(lo, timeutils.utcnow())
                if age < SAMPLES_RECENT_MAXAGE:
                    ts_range_is_recent = True

        if sample_filter.metaquery:
            raise ceilometer.NotImplementedError("metaquery not implemented")

        self.resource_q = resource_q
        self.meter_q = meter_q
        self.sample_q = sample_q
        self.ts_range = ts_range
        self.sample_timebucket_range = sample_timebucket_range

        with driver._lease_connection() as c:
            # If we have a message_id, jump straight to the bymessageid table
            if 'message_id' in sample_q:
                self._query_by_messageid(c.connection)
            # If this is a query for recent samples, without a resource_id,
            # then use the SAMPLES_RECENT_INDEX to find matching meter_ids
            elif not ('resource_id' in resource_q) and ts_range_is_recent:
                self._query_with_recent_index(c.connection)
            else:
                self._query_default_strategy(c.connection)


def _build_empty_statistics():
    return models.Statistics(unit='',
                             count=0,
                             min=0,
                             max=0,
                             avg=0,
                             sum=0,
                             period=None,
                             period_start=None,
                             period_end=None,
                             duration=None,
                             duration_start=None,
                             duration_end=None,
                             groupby=None)


class AggregateSampleQuery(SampleQuery):
    def __init__(self, driver, sample_filter, period, groupby, aggregate):
        super(AggregateSampleQuery, self).__init__(driver, sample_filter)
        self.period = period
        self.groupby = groupby
        self.aggregate = aggregate
        self._is_precomputable = None
        self.results = None
        self.results_by_period_start = None

    @staticmethod
    def _update_meter_stats(stat, meter, sample):
        """Do the stats calculation on a requested time bucket in stats dict

        :param stats: dict where aggregated stats are kept
        :param index: time bucket index in stats
        :param meter: meter record as returned from HBase
        :param start_time: query start time
        :param period: length of the time bucket
        """
        vol = sample.volume
        ts = sample.ts
        if meter:
            stat.unit = meter['unit']
        stat.min = min(vol, stat.min or vol)
        stat.max = max(vol, stat.max)
        stat.sum = vol + (stat.sum or 0)
        stat.count += 1
        stat.duration_start = min(ts, stat.duration_start or ts)
        stat.duration_end = max(ts, stat.duration_end or ts)

    def _to_period_start(self, ts):
        period = self.period

        if period:
            offset = int(timeutils.delta_seconds(
                self.start_time, ts) / period) * period
            period_start = self.start_time + datetime.timedelta(0, offset)
        else:
            period_start = self.start_time

        return period_start

    def _get_stats_bucket_for_ts(self, ts):
        period_start = self._to_period_start(ts)
        stats = self.results_by_period_start.get(period_start)
        if not stats:
            if self.period:
                period_end = period_start + datetime.timedelta(
                    0, self.period)
            else:
                period_end = self.period_end

            stats = _build_empty_statistics()
            stats.period = self.period
            stats.period_start = period_start
            stats.period_end = period_end
            self.results.append(stats)
            self.results_by_period_start[period_start] = stats
        return stats

    def _on_sample_row(self, resource, meter, sample):
        """Override sample row handling."""
        ts = sample.ts
        stats_bucket = self._get_stats_bucket_for_ts(ts)
        self._update_meter_stats(stats_bucket, meter, sample)
        if self.timebucket_aggregate:
            self._update_meter_stats(self.timebucket_aggregate, meter, sample)

    def _on_stats_table_row(self, connection, resource, meter_id, meter,
                            stats_table_row):
        """Override stats row, to short-cut aggregates."""
        self.timebucket_aggregate = None

        if not self._is_precomputable:
            return super(AggregateSampleQuery, self)._on_stats_table_row(
                connection, resource, meter_id, meter, stats_table_row)

        statistics = stats_table_row.statistics
        if statistics:
            t_min = statistics['t_min']
            t_max = statistics['t_max']
            if (t_min and t_max and
                    (self._to_period_start(t_min)
                     == self._to_period_start(t_max))):
                stats_bucket = self._get_stats_bucket_for_ts(t_min)

                if meter:
                    stats_bucket.unit = meter['unit']
                stats_bucket.min = min(stats_bucket.min,
                                       statistics['min'] or stats_bucket.min)
                stats_bucket.max = max(stats_bucket.max,
                                       statistics['max'] or stats_bucket.max)
                stats_bucket.sum += (statistics['sum'] or 0)
                stats_bucket.count += (statistics['n'] or 0)
                stats_bucket.duration_start = min(
                    stats_bucket.duration_start,
                    t_min or stats_bucket.duration_start)
                stats_bucket.duration_end = min(
                    stats_bucket.duration_end,
                    t_max or stats_bucket.duration_end)

                return

        # We can compute the statistics for the bucket,
        # even if we can't use them this time
        timebucket = stats_table_row.timebucket

        if _is_at_range_edge(timebucket, range):
            # We won't use aggregates for timebuckets on the edge of range;
            # we could probably improve this test
            pass
        else:
            # TODO(justinsb): Only if sufficiently old (avoid repeated
            # invalidation)
            self.timebucket_aggregate = _build_empty_statistics()

        # Visit the rows, normally
        self._on_sample_timebuckets(connection, resource, meter_id, meter,
                                    [timebucket])

        # Record the stats we computed, if we did
        if self.timebucket_aggregate:
            self._record_aggregate(connection, resource, meter_id, timebucket)
            self.timebucket_aggregate = None

    def _record_aggregate(self, connection, resource, meter_id, timebucket):
        stats = self.timebucket_aggregate

        meter_stats = {}
        meter_stats['resource_id'] = resource.resource_id
        meter_stats['meter_id'] = meter_id
        meter_stats['timebucket'] = timebucket
        meter_stats['statistics'] = statistics = {}
        statistics['n'] = stats.count
        statistics['min'] = stats.min
        statistics['max'] = stats.max
        statistics['sum'] = stats.sum
        statistics['t_min'] = stats.duration_start
        statistics['t_max'] = stats.duration_end

        connection.update(METER_STATS_TABLE, meter_stats)

    def _get_sample_start_time(self, period):
        """Computes the default start time for statistic aggregation.

        This is bizarre: we default to the minimum timestamp seen on any
        sample.  That's unusual - it would be more understandable if we
        just chose a multiple of the period, or even EPOCH_Y2K, for example.

        I filed bug #1372442 to try to get some clarity here, but in the
        meantime, we'll hide this in here.
        """

        if not period:
            return None

        # I think we should just do this:
        # return EPOCH_Y2K

        # XXX: If we don't get a good answer on 1372442, we should cache...
        min_timestamp = None
        with self.driver._lease_connection() as c:
            for row in c.connection.select(RESOURCE_TABLE, {}):
                t = row.first_sample_timestamp
                if min_timestamp:
                    min_timestamp = min(t, min_timestamp)
                else:
                    min_timestamp = t
        return min_timestamp

    def query(self):
        """Return an iterable of models.Statistics instances.

        Items are containing meter statistics described by the query
        parameters. The filter must have a meter value set.
        """

        sample_filter = self.sample_filter

        can_use_summary = True

        if self.groupby:
            can_use_summary = False
            raise ceilometer.NotImplementedError("Group by not implemented.")

        if self.aggregate:
            for a in self.aggregate:
                LOG.debug(_("Got aggregate: %s"), a.func)
            can_use_summary = False
            raise ceilometer.NotImplementedError('Selectable aggregates '
                                                 'not implemented')

        if self.sample_filter.start:
            start_time = self.sample_filter.start
        else:
            start_time = self._get_sample_start_time(self.period)

        if self.sample_filter.end:
            end_time = self.sample_filter.end
        # XXX: This seems wrong (buckets depend on data?)
        # elif meters:
        #    end_time = meters[0][0]['timestamp']
        else:
            end_time = None

        self.results = []
        self.results_by_period_start = {}
        self.period_start = None
        self.period_end = None

        if not self.period:
            self.period = 0
            self.period_start = start_time
            self.period_end = end_time

        self.start_time = start_time
        self.end_time = end_time

        # TODO(justinsb): Tolerate time filters
        self._is_precomputable = can_use_summary
        for k, _v in vars(self.sample_filter).iteritems():
            if k in ['user', 'project', 'meter', 'resource', 'source']:
                # Trivial cases
                pass
            elif k == 'ts':
                # We have special logic, where we don't use precomputation
                # for the boundary cases
                pass
            else:
                LOG.debug(_("Cannot precompute, due to %s"), k)
                self._is_precomputable = False

        super(AggregateSampleQuery, self).query()

        for stats in self.results:
            stats.avg = (stats.sum / float(stats.count))
            stats.duration = (timeutils.delta_seconds(stats.duration_start,
                                                      stats.duration_end))

        return sorted(self.results, key=lambda r: r.period_start)
