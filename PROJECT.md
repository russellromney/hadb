# hadb

hadb is built on the idea that *you already run multiple application servers for HA*. Instead of distributing the database, you can keep it embedded in your application and replicate to S3. Instead of running a 3-5 node cluster, run one leader and one follower with S3 as the source of truth. The result, for most applications, is HA only costs $5-15/month in S3 GET/PUT/storage charges instead of $100s/month for a database cluster.

This is not a new idea. [Litestream](https://litestream.io/) and [LiteFS](https://fly.io/blog/introducing-litefs/) proved the idea (LiteFS features were folded into Litestream more recently). In fact, for SQLite, Litestream is likely the right choice.

hadb aims to generalize the concept to any embedded database, as a library as well as a sidecar, and support it with various storage options, lease holders, databases, and langauge bindings. 

Specifically, hadb is interested in extending the idea to non-SQLite databases, though SQLite is the reference implementation.

## Where hadb shines

hadb is built for cases where HA is an operational need, not a scaling need. Most applications don't outgrow a single database server, they just can't afford for it to go down. Most use cases don't need distributed query execution or sharded writes. They need the database to survive a node failure without someone getting paged at 3am. And they can tolerate a 1s RPO.

That's a different problem than what most HA database products solve. CockroachDB and Vitess are scaling products that happen to also be available.

Instead, hadb is focused on availability that doesn't try to scale beyond what the a single-node database already handles. If SQLite or DuckDB or RocksDB is fast enough on one node, hadb makes sure that node isn't a single point of failure.

This means hadb is the wrong choice if you actually need distributed writes, cross-node joins, or vertical read scaling beyond what a couple of followers can provide. For those problems, a distributed database is the right tool.

## Why embed your database

> Note: hadb supports cluster mode or Litestream/sqld-like sidecar processes, in addition to embedded mode.

When the database lives inside the application process, reads and writes are function calls instead of network round trips. The application opens a local file and queries it directly.

Failover is straightforward: followers continuously replicate from S3, so the database is already on disk when they need to promote. Claiming an S3 lease is all it takes.

The cost model is also different. You're already paying for a HA application, and S3 storage and operations are pretty cheap. Managed HA Postgres starts around $100/month for comparable durability.

## Tradeoffs

**Asynchronous replication.** The leader replicates to S3 on a 1-2 second interval. Writes in that window are lost if the leader crashes. Synchronous replication avoids this, but it's also why managed Postgres needs 3-5 nodes.

**Single writer.** Only the leader writes. Followers forward writes over HTTP. Most managed databases are also single-writer, but this rules out multi-region writes.

**Eventual consistency.** Followers lag 1-2 seconds behind. A write forwarded to the leader won't be visible on the local follower until replication catches up. Applications either accept the lag or forward reads to the leader too, or use read-after-write with bad performance.

**You run it.** No managed service. The database lives in your process, and you're responsible for monitoring and upgrades. The follower handles failover, but you still operate both nodes.

**Embedded database limits.** haqlite inherits SQLite's constraints — single writer, simpler query planner. Fine for most OLTP workloads, not a Postgres replacement for complex analytics. Other databases implemented will also suffer their engine's limitations.

## Comparisons

**vs rqlite**: S3 instead of Raft. Embedded library instead of standalone server. Tradeoff: rqlite is synchronous (no committed write lost), hadb is asynchronous (writes within the replication lag window can be lost on leader crash).

**vs LiteFS**: Library instead of FUSE sidecar. S3 instead of Consul.

**vs Litestream**: Litestream is a separate process for SQLite only. hadb is an embedded library aiming to support multiple databases. Litestream handles replication but not HA — hadb owns the full lifecycle: leader election, promotion, write forwarding, read replicas.

**vs managed databases** (RDS, Neon, PlanetScale): Potentially cheaper. Zero read latency on primary.

## Future directions

- HA duckdb (OLAP is okay with longer sync latency)
- HA rocksdb (Rocksplicator is dead at Pinterest https://github.com/pinterest/rocksplicator)
- HA proxy - put leader-aware proxies in front of a cluster
- Redis or DynamoDB lease management - use HA Redis to manage leases without S3 GET/PUT costs - way cheaper for S3 costs
- S3 Express as lease/storage engine - 10x cheaper operations
- Kuzu as a database with logical replication
