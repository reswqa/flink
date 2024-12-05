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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.extension.eventtime.EventTimerCallback;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

import java.time.Duration;

public class EventTimeWatermarkExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream<Tuple2<String, Long>> source =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new DataGeneratorSource<Tuple2<String, Long>>(
                                                x -> Tuple2.of("hello", x),
                                                10000000,
                                                TypeInformation.of(
                                                        new TypeHint<Tuple2<String, Long>>() {}))),
                                "Operator1")
                        .withParallelism(2);

        // create an event time watermark strategy
        EventTimeWatermarkStrategy<Tuple2<String, Long>> eventTimeWatermarkStrategy =
                EventTimeWatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(
                                Duration.ofSeconds(30L))
                        .withIdleness(Duration.ofMinutes(1L));

        source.process(
                        EventTimeExtension.extractEventTimeAndWatermark(
                                element -> element.f1, eventTimeWatermarkStrategy))
                .keyBy(element -> element.f0)
                .process(
                        new OneInputStreamProcessFunction<Tuple2<String, Long>, String>() {

                            private EventTimeManager eventTimeManager;

                            @Override
                            public void open(NonPartitionedContext<String> ctx) throws Exception {
                                eventTimeManager = EventTimeExtension.getEventTimeManager(ctx);
                            }

                            @Override
                            public void processRecord(
                                    Tuple2<String, Long> record,
                                    Collector<String> output,
                                    PartitionedContext ctx)
                                    throws Exception {
                                eventTimeManager.registerTimer(
                                        eventTimeManager.currentTime() + 100,
                                        (EventTimerCallback)
                                                (timestamp, output1, ctx1) -> {
                                                    System.out.println(
                                                            "Event timer invoked at : "
                                                                    + timestamp);
                                                });
                            }
                        });
        env.execute("EventTimeWatermarkExample");
    }
}
