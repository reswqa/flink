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
import org.apache.flink.processfunction.api.function.ReduceFunction;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputWindowProcessFunction;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.utils.TaggedUnion;
import org.apache.flink.processfunction.api.windowing.window.Window;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utils class for build window. */
public class Windows {

    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE = Class.forName("org.apache.flink.processfunction.builtin.WindowsImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Please ensure that flink-process-function in your class path");
        }
    }

    public static <IN, W extends Window> WindowBuilder<IN, W> builder(
            WindowAssigner<IN, W> windowAssigner) {
        return new WindowBuilder<>(windowAssigner);
    }

    public static <IN1, IN2, W extends Window> TwoInputWindowBuilder<IN1, IN2, W> twoInputBuilder(
            WindowAssigner<TaggedUnion<IN1, IN2>, W> windowAssigner) {
        return new TwoInputWindowBuilder<>(windowAssigner);
    }

    public static class TwoInputWindowBuilder<IN1, IN2, W extends Window> {
        private final WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner;

        private Trigger<TaggedUnion<IN1, IN2>, W> trigger;

        public TwoInputWindowBuilder(WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner) {
            this.assigner = checkNotNull(assigner);
            this.trigger = assigner.getDefaultTrigger();
        }

        public TwoInputWindowBuilder<IN1, IN2, W> withTrigger(
                Trigger<TaggedUnion<IN1, IN2>, W> trigger) {
            this.trigger = checkNotNull(trigger);
            return this;
        }

        public WindowAssigner<TaggedUnion<IN1, IN2>, W> getAssigner() {
            return assigner;
        }

        public Trigger<TaggedUnion<IN1, IN2>, W> getTrigger() {
            return trigger;
        }
    }

    @SuppressWarnings("unchecked")
    public static <IN1, IN2, OUT, W extends Window>
            TwoInputStreamProcessFunction<IN1, IN2, OUT> apply(
                    TwoInputWindowBuilder<IN1, IN2, W> window,
                    TwoInputWindowProcessFunction<Iterable<IN1>, Iterable<IN2>, OUT, W>
                            windowProcessFunction) {
        try {
            return (TwoInputStreamProcessFunction<IN1, IN2, OUT>)
                    INSTANCE.getMethod(
                                    "apply",
                                    TwoInputWindowProcessFunction.class,
                                    WindowAssigner.class,
                                    Trigger.class)
                            .invoke(null, windowProcessFunction, window.assigner, window.trigger);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class WindowBuilder<IN, W extends Window> {
        private final WindowAssigner<IN, W> assigner;

        private Trigger<IN, W> trigger;

        public WindowBuilder(WindowAssigner<IN, W> assigner) {
            this.assigner = checkNotNull(assigner);
            this.trigger = assigner.getDefaultTrigger();
        }

        public WindowBuilder<IN, W> withTrigger(Trigger<IN, W> trigger) {
            this.trigger = checkNotNull(trigger);
            return this;
        }
    }

    @SuppressWarnings("unchecked")
    public static <IN, OUT, W extends Window> SingleStreamProcessFunction<IN, OUT> apply(
            WindowBuilder<IN, W> window,
            WindowProcessFunction<Iterable<IN>, OUT, W> windowProcessFunction) {
        try {
            return (SingleStreamProcessFunction<IN, OUT>)
                    INSTANCE.getMethod(
                                    "process",
                                    WindowProcessFunction.class,
                                    WindowAssigner.class,
                                    Trigger.class)
                            .invoke(null, windowProcessFunction, window.assigner, window.trigger);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <IN, W extends Window> SingleStreamProcessFunction<IN, IN> reduce(
            WindowBuilder<IN, W> window, ReduceFunction<IN> reduceFunction) {
        try {
            return (SingleStreamProcessFunction<IN, IN>)
                    INSTANCE.getMethod(
                                    "reduce",
                                    ReduceFunction.class,
                                    WindowAssigner.class,
                                    Trigger.class)
                            .invoke(null, reduceFunction, window.assigner, window.trigger);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Pre-aggregate elements but provide more context than {@link #reduce(WindowBuilder,
     * ReduceFunction)}.
     */
    @SuppressWarnings("unchecked")
    public static <IN, W extends Window> SingleStreamProcessFunction<IN, IN> reduce(
            WindowBuilder<IN, W> window,
            ReduceFunction<IN> reduceFunction,
            WindowProcessFunction<IN, IN, W> windowProcessFunction) {
        try {
            return (SingleStreamProcessFunction<IN, IN>)
                    INSTANCE.getMethod(
                                    "reduce",
                                    ReduceFunction.class,
                                    WindowProcessFunction.class,
                                    WindowAssigner.class,
                                    Trigger.class)
                            .invoke(
                                    null,
                                    reduceFunction,
                                    windowProcessFunction,
                                    window.assigner,
                                    window.trigger);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        public static <IN> WindowBuilder<IN, Window> ofSliding(
                Time size, Time slide, TimeType timeType) {
            try {
                return new WindowBuilder<>(
                        (WindowAssigner<IN, Window>)
                                INSTANCE.getMethod(
                                                "ofSliding", Time.class, Time.class, TimeType.class)
                                        .invoke(null, size, slide, timeType));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public static <IN> WindowBuilder<IN, Window> ofTumbling(Time size, TimeType timeType) {
            try {
                return new WindowBuilder<>(
                        (WindowAssigner<IN, Window>)
                                INSTANCE.getMethod("ofTumbling", Time.class, TimeType.class)
                                        .invoke(null, size, timeType));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public static <IN1, IN2> TwoInputWindowBuilder<IN1, IN2, Window> ofTwoInputTumbling(
                Time size, TimeType timeType) {
            try {
                return new TwoInputWindowBuilder<>(
                        (WindowAssigner<TaggedUnion<IN1, IN2>, Window>)
                                INSTANCE.getMethod("ofTumbling", Time.class, TimeType.class)
                                        .invoke(null, size, timeType));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public static <IN1, IN2> TwoInputWindowBuilder<IN1, IN2, Window> ofTwoInputSliding(
                Time size, Time slide, TimeType timeType) {
            try {
                return new TwoInputWindowBuilder<>(
                        (WindowAssigner<TaggedUnion<IN1, IN2>, Window>)
                                INSTANCE.getMethod(
                                                "ofSliding", Time.class, Time.class, TimeType.class)
                                        .invoke(null, size, slide, timeType));
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
