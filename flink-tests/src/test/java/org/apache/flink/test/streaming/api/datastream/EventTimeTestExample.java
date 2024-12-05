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

import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

import java.util.Arrays;

public class EventTimeTestExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "test-source");

        source.process(new CountProcessFunction());
    }

    //    public static class CustomProcessFunction
    //            implements OneInputStreamProcessFunction<Integer, Integer> {
    //
    //
    //        @Override
    //        public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
    //            return
    // Collections.singletonList(EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION);
    //        }
    //
    //        @Override
    //        public void processRecord(Integer record, Collector<Integer> output,
    // PartitionedContext ctx)
    //                throws Exception {
    //            long eventTime = getEventTimeFromRecord(record);
    //            LongWatermark eventTimeWatermark =
    // EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(eventTime);
    //            ctx.getNonPartitionedContext()
    //                    .getWatermarkManager()
    //                    .emitWatermark(eventTimeWatermark);
    //        }
    //    }

    //    public static class CustomProcessFunction
    //            implements OneInputStreamProcessFunction<Integer, Integer> {
    //
    //        @Override
    //        public WatermarkHandlingResult onWatermark(
    //                Watermark watermark,
    //                Collector<Integer> output,
    //                NonPartitionedContext<Integer> ctx) throws Exception {
    //            if (EventTimeExtension.isEventTimeWatermark(watermark.getIdentifier())) {
    //                // do something as needed
    //                ...
    //                return WatermarkHandlingResult.PEEK;
    //            } else {
    //                // do something as needed
    //                ...
    //            }
    //        }
    //    }
}
