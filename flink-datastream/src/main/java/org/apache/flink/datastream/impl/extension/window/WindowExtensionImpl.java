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

package org.apache.flink.datastream.impl.extension.window;


import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.join.ReduceFunction;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.extension.window.WindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.trigger.Trigger;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.assigner.GlobalWindowAssigner;
import org.apache.flink.datastream.impl.extension.window.assigner.SlidingEventTimeWindowsAssigner;
import org.apache.flink.datastream.impl.extension.window.assigner.SlidingProcessingTimeWindowsAssigner;
import org.apache.flink.datastream.impl.extension.window.assigner.TumblingEventTimeWindowsAssigner;
import org.apache.flink.datastream.impl.extension.window.assigner.TumblingProcessTimeWindowsAssigner;
import org.apache.flink.datastream.impl.extension.window.function.InternalReduceWindowFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalWindowFunction;
import org.apache.flink.datastream.impl.extension.window.function.PassThroughWindowProcessFunction;

import java.time.Duration;

public class WindowExtensionImpl {
    public static <IN, W extends Window> OneInputStreamProcessFunction<IN, IN> reduce(
            ReduceFunction<IN> reduceFunction,
            WindowAssigner<IN, W> windowAssigner,
            Trigger<IN, W> trigger) {
        return new InternalReduceWindowFunction<>(
                new PassThroughWindowProcessFunction<>(), windowAssigner, trigger, reduceFunction);
    }

    public static <IN, W extends Window> OneInputStreamProcessFunction<IN, IN> reduce(
            ReduceFunction<IN> reduceFunction,
            WindowProcessFunction<IN, IN, W> windowProcessFunction,
            WindowAssigner<IN, W> windowAssigner,
            Trigger<IN, W> trigger) {
        return new InternalReduceWindowFunction<>(
                windowProcessFunction, windowAssigner, trigger, reduceFunction);
    }

    public static <IN, OUT, W extends Window> OneInputStreamProcessFunction<IN, OUT> process(
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
                Duration size, Duration slide, WindowExtension.TimeWindows.TimeType timeType) {
            if (timeType == WindowExtension.TimeWindows.TimeType.PROCESSING) {
                return SlidingProcessingTimeWindowsAssigner.of(size, slide);
            } else if (timeType == WindowExtension.TimeWindows.TimeType.EVENT) {
                return SlidingEventTimeWindowsAssigner.of(size, slide);
            } else {
                throw new IllegalArgumentException("unsupported time type : " + timeType);
            }
        }

        public static WindowAssigner<?, ?> ofTumbling(
                Duration size, WindowExtension.TimeWindows.TimeType timeType) {
            if (timeType == WindowExtension.TimeWindows.TimeType.PROCESSING) {
                return TumblingProcessTimeWindowsAssigner.of(size);
            } else if (timeType == WindowExtension.TimeWindows.TimeType.EVENT) {
                return TumblingEventTimeWindowsAssigner.of(size);
            } else {
                throw new IllegalArgumentException("unsupported time type : " + timeType);
            }
        }
    }
}
