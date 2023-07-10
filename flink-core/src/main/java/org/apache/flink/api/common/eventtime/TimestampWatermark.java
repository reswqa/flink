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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.api.common.typeutils.GeneralizedWatermarkTypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class TimestampWatermark implements GeneralizedWatermark<TimestampWatermark> {
    private final long timestamp;

    public static final TimestampWatermark MAX_WATERMARK = new TimestampWatermark(Long.MAX_VALUE);

    public static final TimestampWatermark MIN_WATERMARK = new TimestampWatermark(Long.MIN_VALUE);

    public static final GeneralizedWatermarkDeclaration DECLARATION =
            new GeneralizedWatermarkDeclaration(
                    TimestampWatermark.class,
                    () -> TimestampWatermark.MIN_WATERMARK,
                    () -> TimestampWatermark.MAX_WATERMARK,
                    new WatermarkAlignmentStrategy(
                            WatermarkAlignmentStrategy.WatermarkAlignmentMode.NON_BLOCKING),
                    TimestampWatermarkSerializer.INSTANCE);

    public TimestampWatermark(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(TimestampWatermark other) {
        return Long.compare(getTimestamp(), other.getTimestamp());
    }

    private static class TimestampWatermarkSerializer
            extends GeneralizedWatermarkTypeSerializer<TimestampWatermark> {

        private static final TimestampWatermarkSerializer INSTANCE =
                new TimestampWatermarkSerializer();

        @Override
        public TypeSerializer<TimestampWatermark> duplicate() {
            // this is not stateful.
            return this;
        }

        @Override
        public TimestampWatermark createInstance() {
            return new TimestampWatermark(TimestampWatermark.MIN_WATERMARK.getTimestamp());
        }

        @Override
        public TimestampWatermark copy(TimestampWatermark from) {
            // ths is immutable
            return from;
        }

        @Override
        public TimestampWatermark copy(TimestampWatermark from, TimestampWatermark reuse) {
            // ths is immutable
            return from;
        }

        @Override
        public int getLength() {
            return Long.BYTES;
        }

        @Override
        public void serialize(TimestampWatermark record, DataOutputView target) throws IOException {
            target.writeLong(record.timestamp);
        }

        @Override
        public TimestampWatermark deserialize(DataInputView source) throws IOException {
            return new TimestampWatermark(source.readLong());
        }

        @Override
        public TimestampWatermark deserialize(TimestampWatermark reuse, DataInputView source)
                throws IOException {
            return new TimestampWatermark(source.readLong());
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj.getClass().equals(this.getClass());
        }
    }
}
