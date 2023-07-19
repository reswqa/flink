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

package org.apache.flink.processfunction.windows.assigner;

import org.apache.flink.api.common.SerializerContext;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.windows.trigger.EventTimeTrigger;
import org.apache.flink.processfunction.windows.window.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;

import java.util.Collection;
import java.util.Collections;

public class TumblingEventTimeWindowsAssigner extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long size;

    private final long globalOffset;

    private Long staggerOffset = null;

    private final WindowStagger windowStagger;

    private final long allowedLateness = 0;

    protected TumblingEventTimeWindowsAssigner(
            long size, long offset, WindowStagger windowStagger) {
        if (Math.abs(offset) >= size) {
            throw new IllegalArgumentException(
                    "TumblingEventTimeWindows parameters must satisfy abs(offset) < size");
        }

        this.size = size;
        this.globalOffset = offset;
        this.windowStagger = windowStagger;
    }

    @Override
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            if (staggerOffset == null) {
                staggerOffset =
                        windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
            }
            // Long.MIN_VALUE is currently assigned when no timestamp is present
            long start =
                    TimeWindow.getWindowStartWithOffset(
                            timestamp, (globalOffset + staggerOffset) % size, size);
            return Collections.singletonList(new TimeWindow(start, start + size, true));
        } else {
            throw new RuntimeException(
                    "Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
                            + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
                            + "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger() {
        return EventTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "TumblingEventTimeWindows(" + size + ")";
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(SerializerContext serializerContext) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

    /**
     * Creates a new {@code TumblingEventTimeWindows} {@link
     * org.apache.flink.streaming.api.windowing.assigners.WindowAssigner} that assigns elements to
     * time windows based on the element timestamp.
     *
     * @param size The size of the generated windows.
     * @return The time policy.
     */
    public static TumblingEventTimeWindowsAssigner of(Time size) {
        return new TumblingEventTimeWindowsAssigner(
                size.toMilliseconds(), 0, WindowStagger.ALIGNED);
    }

    /**
     * Creates a new {@code TumblingEventTimeWindows} {@link
     * org.apache.flink.streaming.api.windowing.assigners.WindowAssigner} that assigns elements to
     * time windows based on the element timestamp and offset.
     *
     * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes of
     * each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get time
     * windows start at 0:15:00,1:15:00,2:15:00,etc.
     *
     * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time, such as
     * China which is using UTC+08:00,and you want a time window with size of one day, and window
     * begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.
     * The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than
     * UTC time.
     *
     * @param size The size of the generated windows.
     * @param offset The offset which window start would be shifted by.
     */
    public static TumblingEventTimeWindowsAssigner of(Time size, Time offset) {
        return new TumblingEventTimeWindowsAssigner(
                size.toMilliseconds(), offset.toMilliseconds(), WindowStagger.ALIGNED);
    }

    /**
     * Creates a new {@code TumblingEventTimeWindows} {@link
     * org.apache.flink.streaming.api.windowing.assigners.WindowAssigner} that assigns elements to
     * time windows based on the element timestamp, offset and a staggering offset, depending on the
     * staggering policy.
     *
     * @param size The size of the generated windows.
     * @param offset The globalOffset which window start would be shifted by.
     * @param windowStagger The utility that produces staggering offset in runtime.
     */
    public static TumblingEventTimeWindowsAssigner of(
            Time size, Time offset, WindowStagger windowStagger) {
        return new TumblingEventTimeWindowsAssigner(
                size.toMilliseconds(), offset.toMilliseconds(), windowStagger);
    }
}
