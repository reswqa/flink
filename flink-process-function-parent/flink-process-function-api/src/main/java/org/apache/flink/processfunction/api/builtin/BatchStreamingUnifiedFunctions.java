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

package org.apache.flink.processfunction.api.builtin;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.api.function.ReduceFunction;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** This class provides some built-in functions for convenience. */
public final class BatchStreamingUnifiedFunctions {

    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE =
                    Class.forName(
                            "org.apache.flink.processfunction.builtin.BatchStreamingUnifiedFunctionsImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Please ensure that flink-process-function in your class path");
        }
    }

    @SuppressWarnings("unchecked")
    public static <IN, OUT> SingleStreamProcessFunction<IN, OUT> map(MapFunction<IN, OUT> mapFunc) {
        try {
            return (SingleStreamProcessFunction<IN, OUT>)
                    INSTANCE.getMethod("map", MapFunction.class).invoke(null, mapFunc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> SingleStreamProcessFunction<T, T> filter(FilterFunction<T> filterFunc) {
        try {
            return (SingleStreamProcessFunction<T, T>)
                    INSTANCE.getMethod("filter", FilterFunction.class).invoke(null, filterFunc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> SingleStreamProcessFunction<T, T> reduce(ReduceFunction<T> reduceFunc) {
        try {
            return (SingleStreamProcessFunction<T, T>)
                    INSTANCE.getMethod("reduce", ReduceFunction.class).invoke(null, reduceFunc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <T, O>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O> union(
                    SingleStreamProcessFunction<T, O> function,
                    NonKeyedPartitionStream<T>... streams) {
        Preconditions.checkArgument(streams.length >= 2, "Union requires at least two streams.");
        try {
            return (NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O>)
                    INSTANCE.getMethod(
                                    "union",
                                    SingleStreamProcessFunction.class,
                                    NonKeyedPartitionStream[].class)
                            .invoke(null, function, streams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <T, O>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O> union(
                    SingleStreamProcessFunction<T, O> function, GlobalStream<T>... streams) {
        Preconditions.checkArgument(streams.length >= 2, "Union requires at least two streams.");
        try {
            return (NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O>)
                    INSTANCE.getMethod(
                                    "union",
                                    SingleStreamProcessFunction.class,
                                    GlobalStream[].class)
                            .invoke(null, function, streams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <K, T, O>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O> union(
                    SingleStreamProcessFunction<T, O> function,
                    KeyedPartitionStream<K, T>... streams) {
        Preconditions.checkArgument(streams.length >= 2, "Union requires at least two streams.");
        try {
            return (NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<O>)
                    INSTANCE.getMethod(
                                    "union",
                                    SingleStreamProcessFunction.class,
                                    KeyedPartitionStream[].class)
                            .invoke(null, function, streams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <K, NEW_K, T, O>
            KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream<NEW_K, O> union(
                    SingleStreamProcessFunction<T, O> function,
                    KeySelector<O, NEW_K> keySelector,
                    KeyedPartitionStream<K, T>... streams) {
        Preconditions.checkArgument(streams.length >= 2, "Union requires at least two streams.");
        Preconditions.checkNotNull(keySelector);
        // TODO impl this.
        return null;
    }

    @FunctionalInterface
    public interface MapFunction<T, O> extends Function {

        /**
         * The mapping method. Takes an element from the input data set and transforms it into
         * exactly one element.
         *
         * @param value The input value.
         * @return The transformed value
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *     operation to fail and may trigger recovery.
         */
        O map(T value) throws Exception;
    }

    @FunctionalInterface
    public interface FilterFunction<T> extends Function, Serializable {

        /**
         * The filter function that evaluates the predicate.
         *
         * <p><strong>IMPORTANT:</strong> The system assumes that the function does not modify the
         * elements on which the predicate is applied. Violating this assumption can lead to
         * incorrect results.
         *
         * @param value The value to be filtered.
         * @return True for values that should be retained, false for values to be filtered out.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *     operation to fail and may trigger recovery.
         */
        boolean filter(T value) throws Exception;
    }
}
