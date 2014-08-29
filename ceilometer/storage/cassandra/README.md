Notes on cassandra

The partition key determines the mapping to a server in the cluster.
Range-queries are not encouraged.

The remaining primary-key columns form the clustering key; these determine the
order that things are stored on-disk (for each partition key value).
Range-queries are efficient here, as long as the partition-key is specified.

