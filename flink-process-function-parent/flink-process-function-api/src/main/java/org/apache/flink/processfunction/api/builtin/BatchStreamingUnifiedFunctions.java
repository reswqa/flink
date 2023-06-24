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
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;

import java.io.Serializable;
import java.util.function.Consumer;

/** This class provides some built-in functions for convenience. */
public final class BatchStreamingUnifiedFunctions {

    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE = Class.forName("org.apache.flink.processfunction.FunctionImpl");
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

    public static <T> TwoInputStreamProcessFunction<T, T, T> union() {
        return new TwoInputStreamProcessFunction<T, T, T>() {
            @Override
            public void processFirstInputRecord(T record, Consumer<T> output, RuntimeContext ctx) {
                output.accept(record);
            }

            @Override
            public void processSecondInputRecord(T record, Consumer<T> output, RuntimeContext ctx) {
                output.accept(record);
            }
        };
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

    @FunctionalInterface
    public interface ReduceFunction<T> extends Function, Serializable {

        /**
         * The core method of ReduceFunction, combining two values into one value of the same type.
         * The reduce function is consecutively applied to all values of a group until only a single
         * value remains.
         *
         * @param value1 The first value to combine.
         * @param value2 The second value to combine.
         * @return The combined value of both input values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *     operation to fail and may trigger recovery.
         */
        T reduce(T value1, T value2) throws Exception;
    }
}
