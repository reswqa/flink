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

package org.apache.flink.processfunction.api.function;

import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.State;
import org.apache.flink.processfunction.api.StateDescriptor;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

// TODO: consider moving implementations to outside api module
public final class Functions {
    public static <IN, OUT> SingleStreamProcessFunction<IN, OUT> map(Function<IN, OUT> mapFunc) {
        return new SingleStreamProcessFunction<IN, OUT>() {
            @Override
            public void processRecord(IN record, Consumer<OUT> output, RuntimeContext ctx) {
                output.accept(mapFunc.apply(record));
            }
        };
    }

    public static <T> SingleStreamProcessFunction<T, T> filter(Function<T, Boolean> filterFunc) {
        return new SingleStreamProcessFunction<T, T>() {
            @Override
            public void processRecord(T record, Consumer<T> output, RuntimeContext ctx) {
                if (filterFunc.apply(record)) {
                    output.accept(record);
                }
            }
        };
    }

    public static <T> SingleStreamProcessFunction<T, T> reduce(BiFunction<T, T, T> reduceFunc) {
        return new SingleStreamProcessFunction<T, T>() {
            private final StateDescriptor stateDescriptor = null;

            @Override
            public Map<String, StateDescriptor> usesStates() {
                return Collections.singletonMap("reduceState", stateDescriptor);
            }

            @Override
            public void processRecord(T record, Consumer<T> output, RuntimeContext ctx) {
                State state = ctx.getState("reduceState");
                // TODO:
                //  T result = reduceFunc.apply(state.getValue(), record);
                //  state.setValue(result);
                //  if (ctx.getExecutionMode() == STREAM) {
                //      output.accept(result);
                //  }
            }

            // TODO:
            //  public void endOfInput(Consumer<T> output, RuntimeContext ctx) {
            //      if (ctx.getExecutionMode() == BATCH) {
            //          State state = ctx.getState("reduceState");
            //          output.accept(state.getValue());
            //      }
            //  }
        };
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
}
