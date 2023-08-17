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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.processfunction.api.builtin.Windows;
import org.apache.flink.processfunction.api.function.ReduceFunction;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.window.Window;
import org.apache.flink.processfunction.functions.InternalReduceWindowFunction;
import org.apache.flink.processfunction.functions.InternalWindowFunction;
import org.apache.flink.processfunction.functions.PassThroughWindowProcessFunction;
import org.apache.flink.processfunction.windows.assigner.GlobalWindowAssigner;
import org.apache.flink.processfunction.windows.assigner.SlidingEventTimeWindowsAssigner;
import org.apache.flink.processfunction.windows.assigner.SlidingProcessingTimeWindowsAssigner;
import org.apache.flink.processfunction.windows.assigner.TumblingEventTimeWindowsAssigner;
import org.apache.flink.processfunction.windows.assigner.TumblingProcessTimeWindowsAssigner;

public class WindowsImpl {
    public static <IN, W extends Window> SingleStreamProcessFunction<IN, IN> reduce(
            ReduceFunction<IN> reduceFunction,
            WindowAssigner<IN, W> windowAssigner,
            Trigger<IN, W> trigger) {
        return new InternalReduceWindowFunction<>(
                new PassThroughWindowProcessFunction<>(), windowAssigner, trigger, reduceFunction);
    }

    public static <IN, W extends Window> SingleStreamProcessFunction<IN, IN> reduce(
            ReduceFunction<IN> reduceFunction,
            WindowProcessFunction<IN, IN, W> windowProcessFunction,
            WindowAssigner<IN, W> windowAssigner,
            Trigger<IN, W> trigger) {
        return new InternalReduceWindowFunction<>(
                windowProcessFunction, windowAssigner, trigger, reduceFunction);
    }

    public static <IN, OUT, W extends Window> SingleStreamProcessFunction<IN, OUT> process(
            WindowProcessFunction<Iterable<IN>, OUT, W> processFunction,
            WindowAssigner<IN, W> windowAssigner,
            Trigger<IN, W> trigger) {
        return new InternalWindowFunction<>(processFunction, windowAssigner, trigger);
    }

    public static class GlobalWindowsImpl {
        public static WindowAssigner<?, ?> create() {
            return GlobalWindowAssigner.create();
        }
    }

    public static class TimeWindowsImpl {
        public static WindowAssigner<?, ?> ofSliding(
                Time size, Time slide, Windows.TimeWindows.TimeType timeType) {
            if (timeType == Windows.TimeWindows.TimeType.PROCESSING) {
                return SlidingProcessingTimeWindowsAssigner.of(size, slide);
            } else if (timeType == Windows.TimeWindows.TimeType.EVENT) {
                return SlidingEventTimeWindowsAssigner.of(size, slide);
            } else {
                throw new IllegalArgumentException("unsupported time type : " + timeType);
            }
        }

        public static WindowAssigner<?, ?> ofTumbling(
                Time size, Windows.TimeWindows.TimeType timeType) {
            if (timeType == Windows.TimeWindows.TimeType.PROCESSING) {
                return TumblingProcessTimeWindowsAssigner.of(size);
            } else if (timeType == Windows.TimeWindows.TimeType.EVENT) {
                return TumblingEventTimeWindowsAssigner.of(size);
            } else {
                throw new IllegalArgumentException("unsupported time type : " + timeType);
            }
        }
    }
}
