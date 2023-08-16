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

package org.apache.flink.processfunction.builtin;

import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.functions.SingleStreamFilterFunction;
import org.apache.flink.processfunction.functions.SingleStreamMapFunction;
import org.apache.flink.processfunction.functions.SingleStreamReduceFunction;
import org.apache.flink.processfunction.stream.GlobalStreamImpl;
import org.apache.flink.processfunction.stream.KeyedPartitionStreamImpl;
import org.apache.flink.processfunction.stream.NonKeyedPartitionStreamImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BatchStreamingUnifiedFunctionsImpl {
    public static <IN, OUT> SingleStreamProcessFunction<IN, OUT> map(
            org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions.MapFunction<
                            IN, OUT>
                    mapFunc) {
        return new SingleStreamMapFunction<>(mapFunc);
    }

    public static <T> SingleStreamProcessFunction<T, T> filter(
            org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions
                                    .FilterFunction<
                            T>
                    filterFunc) {
        return new SingleStreamFilterFunction<>(filterFunc);
    }

    public static <T> SingleStreamProcessFunction<T, T> reduce(
            org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions
                                    .ReduceFunction<
                            T>
                    reduceFunc) {
        return new SingleStreamReduceFunction<>(reduceFunc);
    }

    // ------------------------------------------------------------
    //                          Union
    // ------------------------------------------------------------
    @SafeVarargs
    public static <T, O>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O> union(
                    SingleStreamProcessFunction<T, O> function,
                    NonKeyedPartitionStream<T>... streams) {
        Iterator<NonKeyedPartitionStream<T>> iterator = Arrays.stream(streams).iterator();
        NonKeyedPartitionStreamImpl<T> bashStream =
                (NonKeyedPartitionStreamImpl<T>) iterator.next();
        List<NonKeyedPartitionStream<T>> otherStreams = new ArrayList<>();
        while (iterator.hasNext()) {
            otherStreams.add(iterator.next());
        }
        return bashStream.union(otherStreams).process(function);
    }

    @SafeVarargs
    public static <K, T, O>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O> union(
                    SingleStreamProcessFunction<T, O> function,
                    KeyedPartitionStream<K, T>... streams) {
        Iterator<KeyedPartitionStream<K, T>> iterator = Arrays.stream(streams).iterator();
        KeyedPartitionStreamImpl<K, T> bashStream =
                (KeyedPartitionStreamImpl<K, T>) iterator.next();
        List<KeyedPartitionStream<K, T>> otherStreams = new ArrayList<>();
        while (iterator.hasNext()) {
            otherStreams.add(iterator.next());
        }
        return bashStream.union(otherStreams).process(function);
    }

    @SafeVarargs
    public static <T, O> GlobalStream.ProcessConfigurableAndGlobalStream<O> union(
            SingleStreamProcessFunction<T, O> function, GlobalStream<T>... streams) {
        Iterator<GlobalStream<T>> iterator = Arrays.stream(streams).iterator();
        GlobalStreamImpl<T> bashStream = (GlobalStreamImpl<T>) iterator.next();
        List<GlobalStream<T>> otherStreams = new ArrayList<>();
        while (iterator.hasNext()) {
            otherStreams.add(iterator.next());
        }
        return bashStream.union(otherStreams).process(function);
    }
}
