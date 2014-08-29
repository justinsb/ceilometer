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
""" Cassandra helpers relating to bucketing
"""
import hashlib

from oslo.utils import timeutils

SECONDS_IN_DAY = 24 * 60 * 60


class Timebucket(object):
    """Buckets time to an integer, with a configurable timespan per step."""
    def __init__(self, seconds, base):
        self.seconds = seconds
        self.base = base

    def to_bucket(self, t):
        delta = timeutils.delta_seconds(self.base, t)
        return int(delta / self.seconds)

    def map_range(self, ts_range):
        bucket_range = {}
        if ts_range:
            lo = ts_range.get('>') or ts_range.get('>=')
            if lo:
                bucket_range['>='] = self.to_bucket(lo)
            hi = ts_range.get('<') or ts_range.get('<=')
            if hi:
                bucket_range['<='] = self.to_bucket(hi)
        return bucket_range

    def convert(self, bucket, convert_to):
        if self.base != convert_to.base:
            raise Exception("Mismatched timebucket bases")
        ratio = convert_to.seconds / self.seconds
        if ratio != int(ratio):
            raise Exception("Non-integer timebucket ratios")
        return bucket / ratio


class HashShard(object):
    """Hashes a value to an integer in range [0, modulo0)."""
    def __init__(self, modulo):
        self.modulo = modulo

    def to_shard(self, v):
        h = hashlib.md5()
        h.update(v)
        hashvalue = int(h.hexdigest(), 16)
        return int(hashvalue % self.modulo)
