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
import operator

from oslo.utils import timeutils
import six

from ceilometer.event.storage import models
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.storage import base
from ceilometer.storage.cassandra import buckets
from ceilometer.storage.cassandra import connection
from ceilometer.storage.cassandra import migration
from ceilometer import utils


LOG = log.getLogger(__name__)


EPOCH_Y2K = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, None)


AVAILABLE_CAPABILITIES = {
    'events': {'query': {'simple': True}},
}


AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': False},
}


KEYSPACE = connection.CassandraKeyspace("events")


# TODO(justinsb): Move message_id to a new index (high cardinality)
EVENT_TABLE = connection.CassandraTable(
    KEYSPACE,
    "event",
    ['timebucket_and_shard', 'ts', 'message_id'],
    {
        'timebucket_and_shard': 'int',
        'ts': 'timestamp',
        'message_id': 'text',
        'event_type': 'text',
        'traits': 'map<text, text>'
    },
    indexes=[
        ['message_id']
    ]
)

EVENT_TABLE_SHARDS_SHIFT = 5
EVENT_TABLE_SHARD = buckets.HashShard(1 << EVENT_TABLE_SHARDS_SHIFT)

EVENT_TABLE_TIMEBUCKET = buckets.Timebucket(300, EPOCH_Y2K)

EVENT_TYPE_TABLE = connection.CassandraTable(
    KEYSPACE,
    "event_type",
    ['event_type'],
    {
        'event_type': 'text',
        'trait_keys': 'set<text>'
    }
)


# oslo isotime assumes UTC, when UTC is not specified.
_ISO8601_TIME_FORMAT_SUBSECOND = '%Y-%m-%dT%H:%M:%S.%f'


def _isotime(at):
    """Stringify time in ISO 8601 format."""
    st = at.strftime(_ISO8601_TIME_FORMAT_SUBSECOND)
    if at.tzinfo:
        tz = at.tzinfo.tzname(None)
        st += ('Z' if tz == 'UTC' else tz)
    return st


# oslo isotime adds a timezone when none is present
def _parse_isotime(s):
    dt = timeutils.parse_isotime(s)
    if dt.tzinfo and not ('+' in s or 'Z' in s):
        LOG.warn(_("Parsing incorrectly assigned timezone to: %s, removing"),
                 s)
        dt = dt.replace(tzinfo=None)
    return dt


def _encode_timebucket_and_shard(timebucket, shard):
    encoded = (timebucket << EVENT_TABLE_SHARDS_SHIFT) + shard
    return encoded


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

    def record_events(self, event_models):
        """Write the events to Cassandra.

        :param event_models: a list of models.Event objects.
        :return problem_events: a list of events that could not be saved in a
          (reason, event) tuple. From the reasons that are enumerated in
          storage.models.Event only the UNKNOWN_PROBLEM is applicable here.
        """
        problem_events = []

        with self._lease_connection() as c:
            for event_model in event_models:
                ts = event_model.generated
                message_id = event_model.message_id
                event_type = event_model.event_type

                timebucket = EVENT_TABLE_TIMEBUCKET.to_bucket(ts)

                shard = EVENT_TABLE_SHARD.to_shard(message_id)

                event_type_row = {}
                event_type_row['event_type'] = event_type
                event_type_row['trait_keys'] = []

                row = {}
                # row['timebucket'] = timebucket
                # row['shard'] = shard
                row['timebucket_and_shard'] = _encode_timebucket_and_shard(
                    timebucket, shard)
                row['ts'] = ts
                row['message_id'] = message_id
                row['event_type'] = event_type
                if event_model.traits:
                    row['traits'] = traits = {}
                    for trait in event_model.traits:
                        key = "%s+%d" % (trait.name, trait.dtype)
                        v = trait.value
                        if trait.dtype == models.Trait.NONE_TYPE:
                            v = None
                        elif trait.dtype == models.Trait.TEXT_TYPE:
                            v = str(v)
                        elif trait.dtype == models.Trait.INT_TYPE:
                            v = str(v)
                        elif trait.dtype == models.Trait.FLOAT_TYPE:
                            v = str(v)
                        elif trait.dtype == models.Trait.DATETIME_TYPE:
                            v = _isotime(v)
                        else:
                            raise Exception("Unknown dtype: " + trait.dtype)
                        traits[key] = v
                        event_type_row['trait_keys'].append(key)

                try:
                    c.connection.insert(EVENT_TABLE, row)

                    if event_type_row['trait_keys']:
                        c.connection.update(EVENT_TYPE_TABLE,
                                            event_type_row,
                                            merge_columns=['trait_keys'])
                except Exception as ex:
                    LOG.debug(_("Failed to record event: %s") % ex)
                    problem_events.append((models.Event.UNKNOWN_PROBLEM,
                                           event_model))
        return problem_events

    def _build_trait(self, name, dtype, v):
        dtype = int(dtype)
        if dtype == models.Trait.NONE_TYPE:
            v = None
        elif dtype == models.Trait.TEXT_TYPE:
            v = str(v)
        elif dtype == models.Trait.INT_TYPE:
            v = int(v)
        elif dtype == models.Trait.FLOAT_TYPE:
            v = float(v)
        elif dtype == models.Trait.DATETIME_TYPE:
            v = _parse_isotime(v)
        else:
            raise Exception("Unknown dtype: " + dtype)

        return models.Trait(name=name,
                            dtype=dtype,
                            value=v)

    def get_events(self, event_filter):
        """Return an iter of models.Event objects.

        :param event_filter: storage.EventFilter object, consists of filters
          for events that are stored in database.
        """

        q = {}

        ts_range = {}
        timebucket_range = {}
        if event_filter.start_time:
            ts = event_filter.start_time
            ts_range['>='] = ts
            timebucket_range['>='] = EVENT_TABLE_TIMEBUCKET.to_bucket(ts)

        if event_filter.end_time:
            ts = event_filter.end_time
            ts_range['<='] = ts
            timebucket_range['<='] = EVENT_TABLE_TIMEBUCKET.to_bucket(ts)

        if ts_range:
            q['ts'] = ts_range

        shard_match = None
        if event_filter.event_type:
            q['event_type'] = event_filter.event_type
        if event_filter.message_id:
            message_id = event_filter.message_id
            q['message_id'] = message_id
            shard_match = EVENT_TABLE_SHARD.to_shard(message_id)

        match_traits = event_filter.traits_filter

        if timebucket_range:
            # We can't do a range scan on the timebucket key
            timebucket_min = timebucket_range.get('>=', None)
            timebucket_max = timebucket_range.get('<=', None)

            shards = shard_match or range(32)
            timebuckets = []

            if timebucket_min == timebucket_max:
                timebuckets = [timebucket_min]
            elif timebucket_min is None or timebucket_max is None:
                # Not a lot we can do here to avoid a scan
                # So we don't pass down the timebucket
                # TODO(justinsb): We could keep track of min,
                #  or guess 30 days from now as max
                timebuckets = None
                pass
            else:
                # TODO(justinsb): Sanity check the range size?
                timebuckets = range(timebucket_min, timebucket_max + 1)

            if timebuckets is not None:
                in_timebucket_and_shard = []
                for shard in shards:
                    for timebucket in timebuckets:
                        in_timebucket_and_shard.append(
                            _encode_timebucket_and_shard(timebucket, shard))

                if len(in_timebucket_and_shard) == 0:
                    LOG.debug(_("No timebucket / shard combinations"))
                    return []
                elif len(in_timebucket_and_shard) == 1:
                    q['timebucket_and_shard'] = in_timebucket_and_shard[0]
                else:
                    q['timebucket_and_shard'] = {'in': in_timebucket_and_shard}

        event_models = []

        if len(q) == 2 and 'message_id' in q and 'shard' in q:
            # Cassandra is a bit funky here; we have a secondary index
            # on message_id.  It demands we pass ALLOW FILTERING if we
            # specify message_id and shard, but not if we just pass
            # message_id
            # Note that we have to explicitly handle only the case
            # where it is message_id and the message_id-determind shard,
            # as otherwise we might lose a filter
            q = {'message_id': q['message_id']}

        with self._lease_connection() as c:
            for e in c.connection.select(EVENT_TABLE, q):
                traits = []
                if e.traits:
                    for trait_key, trait_value in e.traits.items():
                        trait_name, trait_dtype = trait_key.rsplit('+', 1)
                        trait = self._build_trait(trait_name,
                                                  trait_dtype,
                                                  trait_value)
                        traits.append(trait)

                # Future optimization: push some predicates down into the query
                if match_traits:
                    if not _trait_filter_matches(match_traits, traits):
                        continue

                event_model = models.Event(
                    message_id=e.message_id,
                    event_type=e.event_type,
                    generated=e.ts,
                    traits=sorted(traits,
                                  key=operator.attrgetter('dtype'))
                )
                event_models.append(event_model)

        return sorted(event_models, key=operator.attrgetter('generated'))

    def get_event_types(self):
        """Return all event types as an iterable of strings."""
        q = {}
        with self._lease_connection() as c:
            for e in c.connection.select(EVENT_TYPE_TABLE, q):
                yield e.event_type

    def get_trait_types(self, event_type):
        """Return a dictionary containing the name and data type of the trait.

        Only trait types for the provided event_type are returned.

        :param event_type: the type of the Event
        """
        q = {}
        q['event_type'] = event_type

        trait_names = set()

        with self._lease_connection() as c:
            for e in c.connection.select(EVENT_TYPE_TABLE, q):
                for trait_key in e.trait_keys:
                    trait_name, trait_type = trait_key.rsplit('+', 1)
                    if trait_name not in trait_names:
                        trait_names.add(trait_name)
                        data_type = models.Trait.get_name_by_type(
                            int(trait_type))
                        yield {'name': trait_name, 'data_type': data_type}

    def get_traits(self, event_type, filter_trait_name=None):
        """Return all trait instances associated with an event_type.

        If filter_trait_name is specified, only return instances of that
        trait name.
        :param event_type: the type of the Event to filter by
        :param filter_trait_name: the name of the Trait to filter by
        """
        # This is a truly pathological request (for every storage engine)
        # XXX: Should we try to do better?  When is this called?

        q = {}
        q['event_type'] = event_type

        with self._lease_connection() as c:
            for e in c.connection.select(EVENT_TABLE, q):
                for trait_key, trait_value in e.traits.items():
                    trait_name, trait_dtype = trait_key.rsplit('+', 1)
                    if filter_trait_name:
                        if filter_trait_name != trait_name:
                            continue
                    yield self._build_trait(trait_name,
                                            trait_dtype,
                                            trait_value)


def _trait_filter_matches(query, traits):
    for trait_query in query:
        matched_trait = False

        match_op = 'eq'
        match_name = None
        match_type = None
        match_values = None
        for k, v in six.iteritems(trait_query):
            if k == 'key':
                match_name = v
            elif k == 'op':
                match_op = v
            elif k == 'string':
                match_type = models.Trait.TEXT_TYPE
                match_values = v
            elif k == 'integer':
                match_type = models.Trait.INT_TYPE
                match_values = v
            elif k == 'datetime':
                match_type = models.Trait.DATETIME_TYPE
                match_values = v
            elif k == 'float':
                match_type = models.Trait.FLOAT_TYPE
                match_values = v
            else:
                raise Exception("Unknown key in query: " + k)

        for trait in traits:
            if match_name:
                if trait.name != match_name:
                    continue
            if match_type:
                if trait.dtype != match_type:
                    continue

            actual_value = trait.value

            if not hasattr(match_values, '__iter__'):
                match_values = [match_values]

            for match_value in match_values:
                if trait.dtype == models.Trait.NONE_TYPE:
                    pass
                elif trait.dtype == models.Trait.TEXT_TYPE:
                    match_value = str(match_value)
                elif trait.dtype == models.Trait.INT_TYPE:
                    match_value = int(match_value)
                elif trait.dtype == models.Trait.FLOAT_TYPE:
                    match_value = float(match_value)
                elif trait.dtype == models.Trait.DATETIME_TYPE:
                    if isinstance(match_value, six.string_types):
                        match_value = _parse_isotime(match_value)
                    actual_value = timeutils.normalize_time(actual_value)
                    match_value = timeutils.normalize_time(match_value)
                else:
                    raise Exception("Unknown type " + trait.dtype)

                # Potential optimization: do comparison as string, for 'eq'
                if match_op == 'eq':
                    if actual_value == match_value:
                        matched_trait = True
                elif match_op == 'ne':
                    if actual_value != match_value:
                        matched_trait = True
                elif match_op == 'lt':
                    if actual_value < match_value:
                        matched_trait = True
                elif match_op == 'le':
                    if actual_value <= match_value:
                        matched_trait = True
                elif match_op == 'gt':
                    if actual_value > match_value:
                        matched_trait = True
                elif match_op == 'ge':
                    if actual_value >= match_value:
                        matched_trait = True
                else:
                    raise Exception("Unknown trait op " + match_op)
        if not matched_trait:
            return False
    return True
