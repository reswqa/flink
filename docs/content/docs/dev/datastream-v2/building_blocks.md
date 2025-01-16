---
title: DataStream, Partitioning and ProcessFunction
weight: 1
type: docs
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

# Building Blocks

DataStream, Partitioning, ProcessFunction are the most fundamental elements of DataStream API and respectively represent:

- What are the types of data streams

- How data is partitioned

- How to perform operations / processing on data streams

They are also the core parts of the fundamental primitives provided by DataStream API.

## DataStream

Data flows on the stream may be divided into multiple partitions. According to how the data is partitioned on the stream, we divide it into the following categories:

- Global Stream: Force single partition/parallelism, and the correctness of data depends on this.

- Partition Stream:
  - Divide data into multiple partitions. State is only available within the partition. One partition can only be processed by one task, but one task can handle one or multiple partitions.
According to the partitioning approach, it can be further divided into the following two categories:

    - Keyed Partition Stream: Each key is a partition, and the partition to which the data belongs is deterministic.

    - Non-Keyed Partition Stream: Each parallelism is a partition, and the partition to which the data belongs is nondeterministic.

- Broadcast Stream: Each partition contains the same data.

## Partitioning

Above we defined the stream and how it is partitioned. The next topic to discuss is how to convert
between different partition types. We call these transformations partitioning.
For example non-keyed partition stream can be transformed to keyed partition stream via a `KeyBy` partitioning.

```java
NonKeyedPartitionStream<Tuple<Integer, String>> stream = xxx;
KeyedPartitionStream<Integer, String> keyedStream = stream.keyBy(record -> record.f0);
```

Overall, we have the following four partitioning:
 
- KeyBy: Let all data be repartitioned according to specified key.

- Shuffle: Repartition and shuffle all data.

- Global: Merge all partitions into one. 

- Broadcast: Force partitions broadcast data to downstream.

The specific transformation relationship is shown in the following table:

| Partitioning | Global | Keyed | NonKeyed |      Broadcast      |
|:------------:|:------:|:-----:|:--------:|:-------------------:|
|    Global    |   ❎    | KeyBy | Shuffle  |      Broadcast     |
|    Keyed     | Global | KeyBy | Shuffle  |      Broadcast      |
|   NonKeyed   | Global | KeyBy | Shuffle  |      Broadcast      |
|  Broadcast   |   ❎   |  ❎   |    ❎    |          ❎        |

(A crossed box indicates that it is not supported or not required)

One thing to note is: broadcast can only be used in conjunction with other inputs and cannot be directly converted to other streams.

## ProcessFunction
Once we have the data stream, we can apply operations on it. The operations that can be performed over
DataStream are collectively called Process Function. It is the only entrypoint for defining all kinds
of processing on the data streams.

### Classification of ProcessFunction
According to the number of input / output, they are classified as follows:

|               Partitioning                |    Number of Inputs    |        Number of Outputs        |
|:-----------------------------------------:|:----------------------:|:-------------------------------:|
|    OneInputStreamProcessFunction          |           1            |                1                |
| TwoInputNonBroadcastStreamProcessFunction |           2            |                1                |
|  TwoInputBroadcastStreamProcessFunction   |           2            |                1                |
|      TwoOutputStreamProcessFunction       |           1            |                2                |

Logically, process functions that support more inputs and outputs can be achieved by combining them, 
but this implementation might be inefficient. If the call for this becomes louder, 
we will consider supporting as many output edges as we want through a mechanism like OutputTag.
But this loses the explicit generic type information that comes with using ProcessFunction.

The case of two input is relatively special, and we have divided it into two categories:

- TwoInputNonBroadcastStreamProcessFunction: Neither of its inputs is a BroadcastStream, so processing only applied to the single partition.
- TwoInputBroadcastStreamProcessFunction: One of its inputs is the BroadcastStream, so the processing of this input is applied to all partitions. While the other side is Keyed/Non-Keyed Stream, it's processing applied to single partition.

DataStream has series of `process` and `connectAndProcess` methods to transform the input stream or connect and transform two input streams via ProcssFunction.

### Requirements for input and output streams

The following two tables list the input and output stream combinations supported by OneInputStreamProcessFunction and TwoOutputStreamProcessFunction respectively.

For OneInputStreamProcessFunction:

| Input Stream |  Output Stream   |  
|:------------:|:----------------:|
|    Global    |      Global      |
|    Keyed     | Keyed / NonKeyed |
|   NonKeyed   |     NonKeyed     |
|  Broadcast   |  Not Supported   |

For TwoOutputStreamProcessFunction:

| Input Stream |             Output Stream             |  
|:------------:|:-------------------------------------:|
|    Global    |            Global + Global            |
|    Keyed     | Keyed + Keyed / Non-Keyed + Non-Keyed |
|   NonKeyed   |          NonKeyed + NonKeyed          |
|  Broadcast   |             Not Supported             |

There are two points to note here:
- When KeyedPartitionStream is used as input, the output can be either a KeyedPartitionStream or NonKeyedPartitionStream.
For general data processing logic, how to partition data is uncertain, we can only expect a NonKeyedPartitionStream.
If we do need a deterministic partition, we can follow it with a KeyBy partitioning.
However, there are times when we know for sure that the partition of records will not change before
and after processing, shuffle cost due to the extra partitioning can be avoided.
To be safe, in this case we ask for a KeySelector for the output data, and the framework
checks at runtime to see if this invariant is broken. The same is true for two output and two input counterparts.
- Broadcast stream cannot be used as a single input.

Things with two inputs is a little more complicated. The following table lists which streams are compatible with each other and the types of streams they output.

A cross(❎) indicates not supported.

|  Output   | Global |       Keyed        | NonKeyed |     Broadcast     |
|:---------:|:------:|:------------------:|:--------:|:-----------------:|
|  Global   | Global |         ❎          |    ❎     |         ❎      |
|   Keyed   |   ❎   | NonKeyed / Keyed   |    ❎     | NonKeyed / Keyed  |
| NonKeyed  |   ❎   |         ❎          | NonKeyed |     NonKeyed      |
| Broadcast |   ❎   |  NonKeyed / Keyed  | NonKeyed |         ❎         |

The reason why the connection between Global Stream and Non-Global Stream is not supported is that the number of partitions of GlobalStream is forced to be 1, but it is generally not 1 for Non-Global Stream, which will cause conflicts when determining the number of partitions of the output stream. If necessary, they should be transformed into mutually compatible streams and then connected.
Connecting two broadcast streams doesn't really make sense, because each parallelism would have exactly same input data from both streams and any process would be duplicated.
The reason why the output of two keyed partition streams can be keyed or non-keyed is the same as we mentioned above in the case of single input.
When we connect two KeyedPartitionStream, they must have the same key type, otherwise we can't decide how to merge the partitions of the two streams. At the same time, things like access state and register timer are also restricted to the partition itself, cross-partition interaction is not meaningful.

The reasons why the connection between KeyedPartitionStream and NonKeyedPartitionStream is not supported are as follows:
The data on KeyedStream is deterministic, but on NonKeyed is not. It is difficult to think of a scenario where the two need to be connected.
This will complicate the state declaration and access rules. A more detailed discussion can be seen in the subsequent state-related sub-FLIP.
If we see that most people have clear demands for this, we can support it in the future.

## Config Process

After defining the process functions, you may want to make some configurations for the properties of this processing.
For example, set the parallelism and name of the process operation, etc.

Therefore, the return value of `process`/`connectAndProcess` meets the following two requirements at the same time:

- It should be a handle, allowing us to configure the previous processing.

- It should be a new DataStream, allowing us to do further processing on it.

The advantage of this is that configurations can be made more conveniently by continuously using `withXXX` . For example:

```java
inputStream
  .process(func1) // do process 1
  .withName("my-process-func") // configure name for process 1
  .withParallelism(2) //  configure parallelism for process 1
  .process(func2) //  do further process 2
```

## Example

Here is an example to show how to use those building blocks to write a flink job.

```java
// create environment
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// create a stream from source
env.fromSource(someSource)
    // map every element x to x + 1.
    .process(new OneInputStreamProcessFunction<Integer, Integer>() {
                    @Override
                    public void processRecord(
                            Integer x,
                            Collector<Integer> output)
                            throws Exception {
                        output.collect(x + 1);
                    }
                })
    // If the sink does not support concurrent writes, we can force the stream to one partition
    .global()
    // sink the stream to some sink
    .toSink(someSink);
// execute the job
env.execute()// create environment
```
