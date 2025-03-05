---
title: "Release Notes - Flink 2.0"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Release notes - Flink 2.0

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 1.20 and Flink 2.0. Please read these notes carefully if you are 
planning to upgrade your Flink version to 2.0.

## New Features & Behavior Changes

### State & Checkpoints

#### Disaggregated State Storage and Management

##### [FLINK-32070](https://issues.apache.org/jira/browse/FLINK-32070)

The past decade has witnessed a dramatic shift in Flink's deployment mode, workload patterns, and hardware improvements. We've moved from the map-reduce era where workers are computation-storage tightly coupled nodes to a cloud-native world where containerized deployments on Kubernetes become standard. To enable Flink's Cloud-Native future, we introduce Disaggregated State Storage and Management that uses remote storage as primary storage in Flink 2.0.

This new architecture solves the following challenges brought in the cloud-native era for Flink.
1. Local Disk Constraints in containerization
2. Spiky Resource Usage caused by compaction in the current state model
3. Fast Rescaling for jobs with large states (hundreds of Terabytes)
4. Light and Fast Checkpoint in a native way

While extending the state store to interact with remote DFS seems like a straightforward solution, it is insufficient due to Flink's existing blocking execution model. To overcome this limitation, Flink 2.0 introduces an asynchronous execution model alongside a disaggregated state backend, as well as newly designed SQL operators performing asynchronous state access in parallel.

#### Native file copy support

##### [FLINK-35886](https://issues.apache.org/jira/browse/FLINK-35886)

Users can now configure Flink to use s5cmd to speed up downloading files from S3 during the recovery process, when using RocksDB, by a factor of 2.

#### Synchronize rescaling with checkpoint creation to minimize reprocessing for the AdaptiveScheduler

##### [FLINK-35549](https://issues.apache.org/jira/browse/FLINK-35549)

This enables the user to synchronize checkpointing and rescaling in the AdaptiveScheduler. New configuration parameters were introduced for the maximum trigger delay and the number of acceptable failed checkpoints before triggering a rescale to make this behavior configurable. These parameters were updated in [FLINK-36015](https://issues.apache.org/jira/browse/FLINK-36015).

### Runtime & Coordination

#### Further Optimization of Adaptive Batch Execution

##### [FLINK-36333](https://issues.apache.org/jira/browse/FLINK-36333), [FLINK-36159](https://issues.apache.org/jira/browse/FLINK-36159)

Flink possesses adaptive batch execution capabilities that optimize execution plans based on runtime information to enhance performance. Key features include dynamic partition pruning, Runtime Filter, and automatic parallelism adjustment based on data volume. In Flink 2.0, we have further strengthened these capabilities with two new optimizations:

*Adaptive Broadcast Join* - Compared to Shuffled Hash Join and Sort Merge Join, Broadcast Join eliminates the need for large-scale data shuffling and sorting, delivering superior execution efficiency. However, its applicability depends on one side of the input being sufficiently small; otherwise, performance or stability issues may arise. During the static SQL optimization phase, accurately estimating the input data volume of a Join operator is challenging, making it difficult to determine whether Broadcast Join is suitable. By enabling adaptive execution optimization, Flink dynamically captures the actual input conditions of Join operators at runtime and automatically switches to Broadcast Join when criteria are met, significantly improving execution efficiency.

*Automatic Join Skew Optimization* - In Join operations, frequent occurrences of specific keys may lead to significant disparities in data volumes processed by downstream Join tasks. Tasks handling larger data volumes can become long-tail bottlenecks, severely delaying overall job execution. Through the Adaptive Skewed Join optimization, Flink leverages runtime statistical information from Join operator inputs to dynamically split skewed data partitions while ensuring the integrity of Join results. This effectively mitigates long-tail latency caused by data skew.

See more details about the capabilities and usages of Flink's [Adaptive Batch Execution](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/adaptive_batch/).

#### Adaptive Scheduler respects `execution.state-recovery.from-local` flag now

##### [FLINK-36201](https://issues.apache.org/jira/browse/FLINK-36201)

AdaptiveScheduler now respects `execution.state-recovery.from-local` flag, which defaults to false. As a result you now need to opt-in to make local recovery work.

#### Align the desired and sufficient resources definition in Executing and WaitForResources states

##### [FLINK-36014](https://issues.apache.org/jira/browse/FLINK-36014)

The new configuration `jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout` for the AdaptiveScheduler was introduced. It defines a duration for which the JobManager delays the scaling operation after a resource change if only sufficient resources are available.

The existing configuration `jobmanager.adaptive-scheduler.min-parallelism-increase` was deprecated and is not used by Flink anymore.

#### Incorrect watermark idleness timeout accounting when subtask is backpressured/blocked

##### [FLINK-35886](https://issues.apache.org/jira/browse/FLINK-35886)

For detecting idleness, the way how idleness timeout is calculated has changed. Previously the time, when source or source's split has been backpressured or blocked due to watermark alignment, was accounted towards the idleness timeout. This could lead to a situation where sources or some splits were incorrectly switching to idle, while they were being unable to make any progress and had some more records to emit, which in turn could result in incorrectly calculated watermarks and erroneous late data. This has been fixed for 2.0.

This change required some API changes, like introduction of `org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context#getInputActivityClock`. However this shouldn't create compatibility problems for users upgrading from prior Flink versions.

### Table & SQL

#### Materialized Table

##### [FLINK-35187](https://issues.apache.org/jira/browse/FLINK-35187)

Materialized Tables represent a cornerstone of our vision to unify stream and batch processing paradigms. These tables enable users to declaratively manage both real-time and historical data through a single pipeline, eliminating the need for separate codebases or workflows.

In this release, with a focus on production-grade operability, we have done critical enhancements to simplify lifecycle management and execution in real-world environments:

**Query Modifications** - Materialized Tables now support schema and query updates, enabling seamless iteration of business logic without reprocessing historical data. This is vital for production scenarios requiring rapid schema evolution and computational adjustments.

**Kubernetes/Yarn Submission** - Beyond standalone clusters, Flink 2.0 extends native support for submitting Materialized Table refresh jobs to YARN and Kubernetes clusters. This allows users to seamlessly integrate refresh workflows into their production-grade infrastructure, leveraging standardized resource management, fault tolerance, and scalability.

**Ecosystem Integration** - Collaborating with the Apache Paimon community, Materialized Tables now integrate natively with Paimon’s lake storage format, combining Flink’s stream-batch compute with Paimon’s high-performance ACID transactions for unified data serving.

By streamlining modifications and execution on production infrastructure, Materialized Tables empower teams to unify streaming and batch pipelines with higher reliability. Future iterations will deepen production support, including integration with a production-ready schedulers to enable policy-driven refresh automation.

#### SQL gateway supports application mode

##### [FLINK-36702](https://issues.apache.org/jira/browse/FLINK-36702)

SQL gateway now supports executing SQL jobs in application mode, serving as a replacement of the removed per-job deployment mode.

#### SQL Syntax Enhancements

##### [FLINK-31836](https://issues.apache.org/jira/browse/FLINK-31836)

Flink SQL now supports C-style escape strings. See the [documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/sql/queries/overview/#syntax) for more details.

A new `QUALIFY` clause has been added as a more concise syntax for filtering outputs of window functions. Demonstrations on this can be found in the [Top-N](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/sql/queries/topn/) and [Deduplication](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/sql/queries/deduplication/) examples.

### Connectors

#### Support Custom Data Distribution for Input Stream of Lookup Join

##### [FLINK-35652](https://issues.apache.org/jira/browse/FLINK-35652)

Lookup Join is an important feature in Flink, It is typically used to enrich a table with data that is queried from an external system.If we interact with the external systems for each incoming record, we incur significant network IO and RPC overhead. Therefore, most connectors introduce caching to reduce the per-record level query overhead, such as Hbase and JDBC. However, because the data distribution of Lookup Join's input stream is arbitrary, the cache hit rate is sometimes unsatisfactory. External systems may have different requirements for data distribution on the Input side, and Flink does not have this knowledge. Flink 2.0 introduce a mechanism for the connector to tell the Flink planner its desired input stream data distribution or partitioning strategy. This can significantly reduce the amount of cached data and improve performance of Lookup Join.

#### Sink with topologies should not participate in unaligned checkpoint

##### [FLINK-36287](https://issues.apache.org/jira/browse/FLINK-36287)

When the sink writer and committer are not chained, it's possible that committables become part of the channel state. However, then it's possible that they are not received before notifyCheckpointComplete. Further, the contract of notifyCheckpointComplete dictates that all side effects need to be committed or we fail on notifyCheckpointComplete. This contract is essential to final checkpoints, so exactly-once sinks don't use unaligned checkpoints anymore to send metadata around transactions now.

### Configuration

#### Migrate string configuration key to ConfigOption

##### [FLINK-34079](https://issues.apache.org/jira/browse/FLINK-34079)

We have deprecated all `setXxx` and `getXxx` methods except `getString(String key, String defaultValue)` and `setString(String key, String value)`, such as: `setInteger`, `setLong`, `getInteger` and `getLong` etc. We strongly recommend users and developers use get and set methods directly.

In addition, we recommend users to use ConfigOption instead of string as key.

#### Remove flink-conf.yaml from flink dist

##### [FLINK-33677](https://issues.apache.org/jira/browse/FLINK-33677)

[FLIP-366](https://cwiki.apache.org/confluence/x/YZuzDw) introduces support for parsing standard YAML files for Flink configuration. A new configuration file named config.yaml, which adheres to the standard YAML format, has been introduced.

In the Flink 2.0, we have removed the old configuration file from the flink-dist, along with support for the old configuration parser. Users are now required to use the new config.yaml file when starting the cluster and submitting jobs. They can utilize the migration tool to assist in migrating legacy configuration files to the new format.

### MISC

#### DataStream V2 API

##### [FLINK-34547](https://issues.apache.org/jira/browse/FLINK-34547)

The DataStream API is one of the two main APIs that Flink provides for writing data processing programs. As an API that was introduced practically since day-1 of the project and has been evolved for nearly a decade, we are observing more and more problems of it. Improvements on these problems require significant breaking changes, which makes in-place refactor impractical. Therefore, we propose to introduce a new set of APIs, the DataStream API V2, to gradually replace the original DataStream API.

In Flink 2.0, we provide the MVP version of the new DataStream V2 API. It contains the low-level building blocks (DataStream, ProcessFunction, Partitioning), context and primitives like state, time service, watermark processing. At the same time, we also provide some high-level extensions, such as window and join. They are more like short-cuts / sugars, without which users can probably still achieve the same behavior by working with the fundamental APIs, but would be a lot easier with the builtin supports.

See DataStream API(V2) [Documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/datastream-v2/overview/) for more details

**NOTICE:** The new DataStream API is currently in the experimental stage and is not yet stable, thus not recommended for production usage at the moment.

#### Java Supports

##### [FLINK-33163](https://issues.apache.org/jira/browse/FLINK-33163)

Starting the 2.0 version, Flink officially supports Java 21.

The default and recommended Java version is changed to Java 17 (previously Java 11). This change mainly affect the docker images and building Flink from sources.

Meanwhile, Java 8 is no longer supported.

#### Serialization Improvements

##### [FLINK-34037](https://issues.apache.org/jira/browse/FLINK-34037)

Flink 2.0 introduces much more efficient built-in serializers for collection types (i.e., Map / List / Set), which is enabled by default.

We have also upgraded Kryo to version 5.6, which is faster and more memory efficient, and has better supports for newer Java versions.

# Highlight Breaking Changes

## API

The following sets of APIs have been completely removed.
- **DataSet API** - Please migrate to DataStream API, or Table API/SQL if applicable. See also "How to Migrate from DataSet to DataStream".
- **Scala DataStream and DataSet API** - Please migrate to the Java DataStream API.
- **SourceFuction, SinkFunction and Sink V1** - Please migrate to Source and Sink V2.
- **TableSoure and TableSink** - Please migrate to DynamicTableSource and DynamicTableSink. See also "User-defined Sources & Sinks".
- **TableSchema, TableColumn and Types** - Please migrate to Schema, Column and DataTypes respectively.

Some deprecated methods have been removed from DataStream API. See also the list of breaking programming APIs.

Some deprecated fields have been removed from REST API. See also the list of breaking REST APIs.

**NOTICE:** You may find some of the removed APIs still exist in the code base, usually in a different package. They are for internal usages only and can be changed / removed anytime without notifications. Please **DO NOT USE** them.

### Connector Adaption Plan

As SourceFunction, SinkFunction and SinkV1 being removed, existing connectors depending on these APIs will not work on the Flink 2.x series. Here’s the plan for adapting the first-party connectors.
- A new version of Kafka, Paimon, JDBC and ElasticSearch connectors, adapted to the API changes, will be released right after the release of Flink 2.0.0.
- We plan to gradually migrate the remaining first-party connectors within 3 subsequent minor releases (i.e., by Flink 2.3).

## Configuration

Configuration options meet the following criteria are removed. See also the list of removed configuration options.
- Annotated as `@Public` and have been deprecated for at least 2 minor releases.
- Annotated as `@PublicEvolving` and have been deprecated for at least 1 minor releases.

The legacy configuration file `flink-conf.yaml` is no longer supported. Please use `config.yaml` with standard YAML format instead. A migration tool is provided to convert a legacy `flink-conf.yaml` into a new `config.yaml`. See "Migrate from flink-conf.yaml to config.yaml" for more details.

Configuration APIs that takes java objects as arguments are removed from `StreamExecutionEnvironment` and `ExecutionConfig`. They should now be set via `Configuration` and `ConfigOption`. See also the list of breaking programming APIs.

To avoid exposing internal interfaces, User-Defined Functions no longer have full access to `ExecutionConfig`. Instead, necessary functions such as `createSerializer()`, `getGlobalJobParameters()` and `isObjectReuseEnabled()` can now be accessed from `RuntimeContext` directly.

## Misc

- State Compatibility is not guaranteed between 1.x and 2.x.
- Java 8 is no longer supported. The minimum Java version supported by Flink now is Java 11.
- The Per-job deployment mode is no longer supported. Please use the Application mode instead.
- Legacy Mode of Hybrid Shuffle is removed.
