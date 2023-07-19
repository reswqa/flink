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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.evictor.Evictor;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.window.Window;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utils class for build window. */
public class Windows {
    public static <IN, W extends Window> WindowBuilder<IN, W> builder(
            WindowAssigner<IN, W> windowAssigner) {
        return new WindowBuilder<>(windowAssigner);
    }

    public static class WindowBuilder<IN, W extends Window> {
        private static final Class<?> INSTANCE;

        static {
            try {
                INSTANCE = Class.forName("org.apache.flink.processfunction.builtin.WindowsImpl");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-process-function in your class path");
            }
        }

        private WindowAssigner<IN, W> assigner;

        private Trigger<IN, W> trigger;

        @Nullable private Evictor<IN, W> evictor;

        public WindowBuilder(WindowAssigner<IN, W> assigner) {
            this.assigner = checkNotNull(assigner);
            this.trigger = assigner.getDefaultTrigger();
        }

        public WindowBuilder<IN, W> withTrigger(Trigger<IN, W> trigger) {
            this.trigger = checkNotNull(trigger);
            return this;
        }

        public WindowBuilder<IN, W> withEvictor(Evictor<IN, W> evictor) {
            this.evictor = checkNotNull(evictor);
            return this;
        }

        @SuppressWarnings("unchecked")
        public SingleStreamProcessFunction<IN, IN> reduce(
                BatchStreamingUnifiedFunctions.ReduceFunction<IN> reduceFunction) {
            try {
                return (SingleStreamProcessFunction<IN, IN>)
                        INSTANCE.getMethod(
                                        "reduce",
                                        BatchStreamingUnifiedFunctions.ReduceFunction.class,
                                        WindowAssigner.class,
                                        Trigger.class,
                                        Evictor.class)
                                .invoke(null, reduceFunction, assigner, trigger, evictor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Pre-aggregate elements but provide more context than {@link
         * #reduce(BatchStreamingUnifiedFunctions.ReduceFunction)}
         */
        @SuppressWarnings("unchecked")
        public SingleStreamProcessFunction<IN, IN> reduce(
                BatchStreamingUnifiedFunctions.ReduceFunction<IN> reduceFunction,
                WindowProcessFunction<IN, IN, W> windowProcessFunction) {
            try {
                return (SingleStreamProcessFunction<IN, IN>)
                        INSTANCE.getMethod(
                                        "reduce",
                                        BatchStreamingUnifiedFunctions.ReduceFunction.class,
                                        WindowProcessFunction.class,
                                        WindowAssigner.class,
                                        Trigger.class,
                                        Evictor.class)
                                .invoke(
                                        null,
                                        reduceFunction,
                                        windowProcessFunction,
                                        assigner,
                                        trigger,
                                        evictor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public <OUT> SingleStreamProcessFunction<IN, OUT> apply(
                WindowProcessFunction<Iterable<IN>, OUT, W> windowProcessFunction) {
            try {
                return (SingleStreamProcessFunction<IN, OUT>)
                        INSTANCE.getMethod(
                                        "process",
                                        WindowProcessFunction.class,
                                        WindowAssigner.class,
                                        Trigger.class,
                                        Evictor.class)
                                .invoke(null, windowProcessFunction, assigner, trigger, evictor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TimeWindows {
        private static final Class<?> INSTANCE;

        static {
            try {
                INSTANCE =
                        Class.forName(
                                "org.apache.flink.processfunction.builtin.WindowsImpl$TimeWindowsImpl");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-process-function in your class path");
            }
        }

        @SuppressWarnings("unchecked")
        public static <IN> WindowAssigner<IN, Window> ofSliding(
                Time size, Time slide, TimeType timeType) {
            try {
                return (WindowAssigner<IN, Window>)
                        INSTANCE.getMethod("ofSliding", Time.class, Time.class, TimeType.class)
                                .invoke(null, size, slide, timeType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public static <IN> WindowAssigner<IN, Window> ofTumbling(Time size, TimeType timeType) {
            try {
                return (WindowAssigner<IN, Window>)
                        INSTANCE.getMethod("ofTumbling", Time.class, TimeType.class)
                                .invoke(null, size, timeType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public enum TimeType {
            PROCESSING,
            EVENT
        }
    }
}
