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
""" Various Cassandra helpers
"""
import datetime
import random
import uuid


def timeuuid_to_timestamp(u):
    return datetime.datetime.fromtimestamp((u.time - 0x01b21dd213814000L)
                                           / 10000000.0)

UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0)


def _unix_time(dt):
    delta = dt - UNIX_EPOCH
    return delta.total_seconds()


def timeuuid_from_timestamp(t):
    seconds_since_epoch = _unix_time(t)
    nanoseconds = int(seconds_since_epoch * 1e9)
    timestamp = int(nanoseconds//100) + 0x01b21dd213814000L

    clock_seq = random.randrange(1 << 14L)

    time_low = timestamp & 0xffffffffL
    time_mid = (timestamp >> 32L) & 0xffffL
    time_hi_version = (timestamp >> 48L) & 0x0fffL
    clock_seq_low = clock_seq & 0xffL
    clock_seq_hi_variant = (clock_seq >> 8L) & 0x3fL

    node = uuid.getnode()

    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                             clock_seq_hi_variant, clock_seq_low, node),
                     version=1)


def make_timestamp_range(start_timestamp, end_timestamp,
                         start_timestamp_op=None, end_timestamp_op=None):

    """Create the query document to find timestamps within that range.

    This is done by given two possible datetimes and their operations.
    By default, using $gte for the lower bound and $lt for the upper bound.
    """
    ts_range = {}

    if start_timestamp:
        start_timestamp_op = start_timestamp_op or ''
        start_timestamp_op = start_timestamp_op.replace('$', '')

        op = None
        if start_timestamp_op == '':
            op = '>='
        elif start_timestamp_op in ['ge', 'gte', '>=']:
            op = '>='
        elif start_timestamp_op in ['gt', '>']:
            op = '>'

        if op is None:
            raise Exception('Unknown operator: ' + start_timestamp_op)
        else:
            ts_range[op] = start_timestamp

    if end_timestamp:
        end_timestamp_op = end_timestamp_op or ''
        end_timestamp_op = end_timestamp_op.replace('$', '')

        op = None
        if end_timestamp_op == '':
            op = '<'
        elif end_timestamp_op in ['le', 'lte', '<=']:
            op = '<='
        elif end_timestamp_op in ['lt', '<']:
            op = '<'

        if op is None:
            raise Exception('Unknown operator: ' + end_timestamp_op)
        else:
            ts_range[op] = end_timestamp

    return ts_range


def escape_column(name):
    return escape_identifier(name)


def escape_table(name):
    return escape_identifier(name)


def escape_identifier(name):
    # TODO(justinsb): Detect CQL3 vs CQL2 - quote character changed?
    quote_char = '"'
    if quote_char in name:
        # XXX: Double-quote or something?
        raise Exception("Quotes in identifiers are not supported")
    name = name.replace(':', '_')
    return quote_char + name + quote_char
