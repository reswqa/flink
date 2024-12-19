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

package org.apache.flink.datastream.impl.extension.window.window;

import org.apache.flink.api.common.memory.DataInputView;
import org.apache.flink.api.common.memory.DataOutputView;
import org.apache.flink.api.common.typeinfo.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeManager;
import org.apache.flink.datastream.api.extension.window.assigner.MergingWindowAssigner;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.trigger.Trigger;
import org.apache.flink.datastream.api.extension.window.window.TimeWindow;
import org.apache.flink.util.MathUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimeWindowImpl implements BoundedWindow<Long>, TimeWindow {
    private final long start;
    private final long end;

    private final boolean isEventTime;

    private EventTimeManager eventTimeManager;

    public TimeWindowImpl(long start, long end, boolean isEventTime) {
        this.start = start;
        this.end = end;
        this.isEventTime = isEventTime;
    }

    /**
     * Gets the starting timestamp of the window. This is the first timestamp that belongs to this
     * window.
     *
     * @return The starting timestamp of this window.
     */
    @Override
    public long getStart() {
        return start;
    }

    /**
     * Gets the end timestamp of this window. The end timestamp is exclusive, meaning it is the
     * first timestamp that does not belong to this window any more.
     *
     * @return The exclusive end timestamp of this window.
     */
    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimeWindowImpl that = (TimeWindowImpl) o;
        return getStart() == that.getStart() && getEnd() == that.getEnd();
    }

    @Override
    public int hashCode() {
        return MathUtils.longToIntWithBitMixing(start + end);
    }

    @Override
    public String toString() {
        return "TimeWindow{" + "start=" + start + ", end=" + end + '}';
    }

    /**
     * Returns {@code true} if this window intersects the given window or if this window is just
     * after or before the given window.
     */
    public boolean intersects(TimeWindowImpl other) {
        return this.start <= other.end && this.end >= other.start;
    }

    /** Returns the minimal window covers both this window and the given window. */
    public TimeWindowImpl cover(TimeWindowImpl other) {
        return new TimeWindowImpl(
                Math.min(start, other.start), Math.max(end, other.end), isEventTime);
    }

    /**
     * Gets the largest timestamp that still belongs to this window.
     *
     * <p>This timestamp is identical to {@code getEnd() - 1}.
     *
     * @return The largest timestamp that still belongs to this window.
     * @see #getEnd()
     */
    @Override
    public Long maxBoundary() {
        if (isEventTime) {
            long cleanupTime = maxTimeStamp() + 0;
            return cleanupTime >= maxTimeStamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return maxTimeStamp();
        }
    }

    @Override
    public boolean isBoundaryReached(WindowAssigner.WindowAssignerContext context) {
        if (isEventTime) {
            return context.getCurrentEventTime() == maxBoundary();
        } else {
            return context.getCurrentProcessingTime() == maxBoundary();
        }
    }

    public boolean isWindowLate(long currentWatermark) {
        return isEventTime && maxBoundary() <= currentWatermark;
    }

    @Override
    public void registerCleaner(Trigger.TriggerContext triggerContext) {
        if (isEventTime) {
            triggerContext.registerEventTimeListener(maxBoundary());
        } else {
            triggerContext.registerProcessingTimeTimer(maxBoundary());
        }
    }

    @Override
    public void unRegisterCleaner(Trigger.TriggerContext triggerContext) {
        if (isEventTime) {
            triggerContext.deleteEventTimeListener(maxBoundary());
        } else {
            triggerContext.deleteProcessingTimeTimer(maxBoundary());
        }
    }

    public long maxTimeStamp() {
        return end - 1;
    }
    // ------------------------------------------------------------------------
    // Serializer
    // ------------------------------------------------------------------------

    /** The serializer used to write the TimeWindow type. */
    public static class Serializer extends TypeSerializerSingleton<TimeWindowImpl> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TimeWindowImpl createInstance() {
            return new TimeWindowImpl(0L, 1L, false);
        }

        @Override
        public TimeWindowImpl copy(TimeWindowImpl from) {
            return from;
        }

        @Override
        public TimeWindowImpl copy(TimeWindowImpl from, TimeWindowImpl reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return Long.BYTES + Long.BYTES;
        }

        @Override
        public void serialize(TimeWindowImpl record, DataOutputView target) throws IOException {
            target.writeLong(record.start);
            target.writeLong(record.end);
            target.writeBoolean(record.isEventTime);
        }

        @Override
        public TimeWindowImpl deserialize(DataInputView source) throws IOException {
            long start = source.readLong();
            long end = source.readLong();
            boolean isEventTime = source.readBoolean();
            return new TimeWindowImpl(start, end, isEventTime);
        }

        @Override
        public TimeWindowImpl deserialize(TimeWindowImpl reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeBoolean(source.readBoolean());
        }

        // ------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<TimeWindowImpl> snapshotConfiguration() {
            return new TimeWindowImpl.Serializer.TimeWindowSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class TimeWindowSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<TimeWindowImpl> {

            public TimeWindowSerializerSnapshot() {
                super(TimeWindowImpl.Serializer::new);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Merge overlapping {@link TimeWindowImpl}s. For use by merging {@link WindowAssigner
     * WindowAssigners}.
     */
    public static void mergeWindows(
            Collection<TimeWindowImpl> windows,
            MergingWindowAssigner.MergeCallback<TimeWindowImpl> c) {

        // sort the windows by the start time and then merge overlapping windows

        List<TimeWindowImpl> sortedWindows = new ArrayList<>(windows);

        Collections.sort(sortedWindows, (o1, o2) -> Long.compare(o1.getStart(), o2.getStart()));

        List<Tuple2<TimeWindowImpl, Set<TimeWindowImpl>>> merged = new ArrayList<>();
        Tuple2<TimeWindowImpl, Set<TimeWindowImpl>> currentMerge = null;

        for (TimeWindowImpl candidate : sortedWindows) {
            if (currentMerge == null) {
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            } else if (currentMerge.f0.intersects(candidate)) {
                currentMerge.f0 = currentMerge.f0.cover(candidate);
                currentMerge.f1.add(candidate);
            } else {
                merged.add(currentMerge);
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            }
        }

        if (currentMerge != null) {
            merged.add(currentMerge);
        }

        for (Tuple2<TimeWindowImpl, Set<TimeWindowImpl>> m : merged) {
            if (m.f1.size() > 1) {
                c.merge(m.f1, m.f0);
            }
        }
    }

    /**
     * Method to get the window start for a timestamp.
     *
     * @param timestamp epoch millisecond to get the window start.
     * @param offset The offset which window start would be shifted by.
     * @param windowSize The size of the generated windows.
     * @return window start
     */
    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        final long remainder = (timestamp - offset) % windowSize;
        // handle both positive and negative cases
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }
}
