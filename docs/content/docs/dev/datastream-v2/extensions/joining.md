---
title: "Joining"
weight: 9 
type: docs
aliases:
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

# Joining

Join is used to merge two data streams by matching elements from both streams based on a common key, 
and performing calculations on the matched elements. 

This section will introduce the Join operation in DataStream V2 in detail.

Please note that currently, DataStream V2 supports only non-window INNER joins. 
Other types of joins, such as interval joins, lookup joins, and window joins, will be supported in the future.

## Non-Window Join

In non-window Join, Flink first creates two built-in states to store the input data, 
specifically for each of the two input data streams. 
When data arrives from one side, Flink attempts to match it with the data stored in the state 
of the other side and performs computations on the matching elements.

During this process, users need to define how to compute the matching elements using [JoinFunction](#joinfunction),
then use the [relevant APIs](#apis-for-performing-join) to perform the Join.

Please note that the Join requires both input streams to be KeyedStream.

### JoinFunction

`JoinFunction` is an interface used to describe how to calculate the matched data. 
It has only one method `processRecord`. Users can get the matched elements in `processRecord` 
to perform calculations and then output the calculation results.

Below is an example demonstrating how to use a `JoinFunction` to connect student personal information with their exam scores.

```java
class JoinStudentInformationAndScore
        implements JoinFunction<StudentInfo, ExamScore, EnrichedStudentExamScore> {

    @Override
    public void processRecord(
            StudentInfo studentInfo,
            ExamScore examScore,
            Collector<EnrichedStudentExamScore> output,
            RuntimeContext ctx)
            throws Exception {
        // do some calculation logic and emit joined result
        EnrichedStudentExamScore studentExamScore = new EnrichedStudentExamScore(studentInfo.getId(), studentInfo.getName(), examScore.getScore());
        output.collect(studentExamScore);
    }
}
```

### APIs for performing Join

After implement the `JoinFunction`, the user should convert `JoinFunction` to `ProcessFunction` that can be processed by DataStream API V2.
An example of converting `JoinFunction` to `ProcessFunction` and use the converted `ProcessFunction` is as follows:

```java
TwoInputNonBroadcastStreamProcessFunction wrappedJoinFunction = BuiltinFuncs.join(new CustomJoinFunction());
NonKeyedPartitionStream joinedStream = keyedStream1.connectAndProcess(
  keyedstream2,
  wrappedJoinFunction
);
```

To make it easier for users, we provide some extended APIs based on the above tool, which have the same functionality but are simpler to use.

1. If the two input data streams are already KeyedStream, the user can directly convert the two 
KeyedStreams and JoinFunction into the data stream after Join.

```java
NonKeyedPartitionStream joinedStream = BuiltinFuncs.join(
  keyedStream1,
  keyedStream2,
  new CustomJoinFunction()
);
```

2. If the two input data streams are NonKeyedStream, users can convert the two NonKeyedStreams 
and the corresponding Join key KeySelector and JoinFunction into the data stream after Join.

```java
NonKeyedPartitionStream joinedStream = BuiltinFuncs.join(
  stream1,
  new CustomJoinKeySelector1(),
  stream2,
  new CustomJoinKeySelector2(),
  new CustomJoinFunction()
);
```

{{< top >}}
