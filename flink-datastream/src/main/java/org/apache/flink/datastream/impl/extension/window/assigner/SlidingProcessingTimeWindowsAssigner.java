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

package org.apache.flink.datastream.impl.extension.window.assigner;

import org.apache.flink.api.common.typeinfo.utils.TypeSerializer;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.trigger.Trigger;
import org.apache.flink.datastream.impl.extension.window.trigger.ProcessingTimeTrigger;
import org.apache.flink.datastream.impl.extension.window.window.TimeWindow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SlidingProcessingTimeWindowsAssigner extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long size;

    private final long offset;

    private final long slide;

    private SlidingProcessingTimeWindowsAssigner(long size, long slide, long offset) {
        if (Math.abs(offset) >= slide || size <= 0) {
            throw new IllegalArgumentException(
                    "SlidingProcessingTimeWindows parameters must satisfy "
                            + "abs(offset) < slide and size > 0");
        }

        this.size = size;
        this.slide = slide;
        this.offset = offset;
    }

    @Override
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        timestamp = context.getCurrentProcessingTime();
        List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - size; start -= slide) {
            windows.add(new TimeWindow(start, start + size, false));
        }
        return windows;
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger() {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "SlidingProcessingTimeWindows(" + size + ", " + slide + ")";
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer() {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    /**
     * Creates a new {@code SlidingProcessingTimeWindows} {@link
     * org.apache.flink.streaming.api.windowing.assigners.WindowAssigner} that assigns elements to
     * sliding time windows based on the element timestamp.
     *
     * @param size The size of the generated windows.
     * @param slide The slide interval of the generated windows.
     * @return The time policy.
     */
    public static SlidingProcessingTimeWindowsAssigner of(Duration size, Duration slide) {
        return new SlidingProcessingTimeWindowsAssigner(
                size.toMillis(), slide.toMillis(), 0);
    }

    /**
     * Creates a new {@code SlidingProcessingTimeWindows} {@link
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
     * @param slide The slide interval of the generated windows.
     * @param offset The offset which window start would be shifted by.
     * @return The time policy.
     */
    public static SlidingProcessingTimeWindowsAssigner of(Duration size, Duration slide, Duration offset) {
        return new SlidingProcessingTimeWindowsAssigner(
                size.toMillis(), slide.toMillis(), offset.toMillis());
    }
}
