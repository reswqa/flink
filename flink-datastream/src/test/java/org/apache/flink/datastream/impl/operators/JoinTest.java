/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;

import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.join.JoinExtension;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.operators.WindowOperatorTest.ValueWithTimestamp;


import org.apache.flink.streaming.api.functions.sink.PrintSink;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;

class JoinTest implements Serializable {
    @Test
    public void testJoin() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        KeyedPartitionStream<Long, Long> source1 = env.fromSource(
                        new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                                new WindowOperatorTest.TestGeneratorFunction(),
                                100_000,
                                TypeInformation.of(ValueWithTimestamp.class))),
                        "source")
                .process(EventTimeExtension.extractEventTime(element -> element.getTimestamp()))
                .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                    @Override
                    public void processRecord(
                            ValueWithTimestamp record,
                            Collector<Long> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect((long) record.getValue());
                    }
                })
                .keyBy(x -> x % 2);

        KeyedPartitionStream<Long, Long> source2 = env.fromSource(
                        new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                                new WindowOperatorTest.TestGeneratorFunction(),
                                100_000,
                                TypeInformation.of(ValueWithTimestamp.class))),
                        "source")
                .process(EventTimeExtension.extractEventTime(element -> element.getTimestamp()))
                .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                    @Override
                    public void processRecord(
                            ValueWithTimestamp record,
                            Collector<Long> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect((long) record.getValue());
                    }
                })
                .keyBy(x -> x % 2);

        source1.connectAndProcess(
                        source2,
                        WindowExtension.apply(
                                WindowExtension.TimeWindows.<Long, Long>ofTwoInputTumbling(
                                        Duration.ofSeconds(5), WindowExtension.TimeWindows.TimeType.EVENT),
                                new JoinFunction<Long, Long, String>() {
                                    @Override
                                    public void processRecord(
                                            Long leftRecord,
                                            Long rightRecord,
                                            Collector<String> output,
                                            RuntimeContext ctx)
                                            throws Exception {
                                        output.collect(
                                                String.format(
                                                        "joined: (%s, %s)",
                                                        leftRecord, rightRecord));
                                    }
                                },
                                JoinExtension.JoinType.INNER))
                .toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("testJoin");
    }
}
