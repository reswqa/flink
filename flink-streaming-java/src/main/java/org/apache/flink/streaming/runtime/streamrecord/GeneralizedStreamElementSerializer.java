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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.api.common.eventtime.GeneralizedStreamElement;
import org.apache.flink.api.common.eventtime.GeneralizedWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermarkWrapper;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.GeneralizedWatermarkTypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GeneralizedStreamElementSerializer<T>
        extends TypeSerializer<GeneralizedStreamElement> {
    private static final long serialVersionUID = 1L;

    private static final int TAG_REC_WITH_TIMESTAMP = 0;
    private static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
    private static final int TAG_WATERMARK = 2;
    private static final int TAG_LATENCY_MARKER = 3;
    private static final int TAG_STREAM_STATUS = 4;
    private static final int TAG_GENERALIZED_WATERMARK = 5;

    private final Map<Class<?>, GeneralizedWatermarkTypeSerializer> generalizedWatermarkSerializers;

    private final TypeSerializer<T> recordSerializer;

    public GeneralizedStreamElementSerializer(
            TypeSerializer<T> recordSerializer,
            Map<Class<?>, GeneralizedWatermarkTypeSerializer> generalizedWatermarkSerializers) {
        this.generalizedWatermarkSerializers = generalizedWatermarkSerializers;
        this.recordSerializer = recordSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<GeneralizedStreamElement> duplicate() {
        TypeSerializer<T> copy = recordSerializer.duplicate();
        return (copy == recordSerializer)
                ? this
                : new GeneralizedStreamElementSerializer<>(copy, generalizedWatermarkSerializers);
    }

    @Override
    public GeneralizedStreamElement createInstance() {
        // TODO only supports reuse for StreamRecord atm.
        return new StreamRecord<T>(recordSerializer.createInstance());
    }

    @Override
    public GeneralizedStreamElement copy(GeneralizedStreamElement from) {
        if (from instanceof StreamElement) {
            StreamElement streamElement = (StreamElement) from;
            // we can reuse the timestamp since Instant is immutable
            if (streamElement.isRecord()) {
                StreamRecord<T> fromRecord = streamElement.asRecord();
                return fromRecord.copy(recordSerializer.copy(fromRecord.getValue()));
            } else if (streamElement.isWatermark()
                    || streamElement.isWatermarkStatus()
                    || streamElement.isLatencyMarker()) {
                // is immutable
                return from;
            } else {
                throw new RuntimeException();
            }
        }
        // only generalized watermark atm.
        GeneralizedWatermark<?> generalizedWatermark = (GeneralizedWatermark<?>) from;
        Class<?> watermarkClazz = generalizedWatermark.getClass();
        if (generalizedWatermark instanceof ProcessWatermarkWrapper) {
            watermarkClazz =
                    ((ProcessWatermarkWrapper) generalizedWatermark)
                            .getProcessWatermark()
                            .getClass();
        }
        TypeSerializer<GeneralizedWatermark> generalizedWatermarkSerializer =
                generalizedWatermarkSerializers.get(watermarkClazz);
        if (generalizedWatermarkSerializer == null) {
            throw new RuntimeException(
                    "Can not serialize generalized watermark for "
                            + generalizedWatermark.getClass().getSimpleName());
        }
        return generalizedWatermarkSerializer.copy(generalizedWatermark);
    }

    @Override
    public GeneralizedStreamElement copy(
            GeneralizedStreamElement from, GeneralizedStreamElement reuse) {
        if (from instanceof StreamElement) {
            Preconditions.checkState(reuse instanceof StreamElement);

            if (((StreamElement) from).isRecord() && ((StreamElement) reuse).isRecord()) {
                StreamRecord<T> fromRecord = ((StreamElement) from).asRecord();
                StreamRecord<T> reuseRecord = ((StreamElement) reuse).asRecord();

                T valueCopy = recordSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
                fromRecord.copyTo(valueCopy, reuseRecord);
                return reuse;
            } else if (((StreamElement) from).isWatermark()
                    || ((StreamElement) from).isWatermarkStatus()
                    || ((StreamElement) from).isLatencyMarker()) {
                // is immutable
                return from;
            } else {
                throw new RuntimeException("Cannot copy " + from + " -> " + reuse);
            }
        }
        // only generalized watermark atm.
        GeneralizedWatermark<?> generalizedWatermark = (GeneralizedWatermark<?>) from;
        Class<?> watermarkClazz = generalizedWatermark.getClass();
        if (generalizedWatermark instanceof ProcessWatermarkWrapper) {
            watermarkClazz =
                    ((ProcessWatermarkWrapper) generalizedWatermark)
                            .getProcessWatermark()
                            .getClass();
        }
        TypeSerializer<GeneralizedWatermark> generalizedWatermarkSerializer =
                generalizedWatermarkSerializers.get(watermarkClazz);
        if (generalizedWatermarkSerializer == null) {
            throw new RuntimeException(
                    "Can not serialize generalized watermark for "
                            + generalizedWatermark.getClass().getSimpleName());
        }
        return generalizedWatermarkSerializer.copy(
                generalizedWatermark, (GeneralizedWatermark) reuse);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(GeneralizedStreamElement value, DataOutputView target)
            throws IOException {
        if (value instanceof StreamElement) {
            if (((StreamElement) value).isRecord()) {
                StreamRecord<T> record = ((StreamElement) value).asRecord();
                if (record.hasTimestamp()) {
                    target.write(TAG_REC_WITH_TIMESTAMP);
                    target.writeLong(record.getTimestamp());
                } else {
                    target.write(TAG_REC_WITHOUT_TIMESTAMP);
                }
                recordSerializer.serialize(record.getValue(), target);
            } else if (((StreamElement) value).isWatermark()) {
                target.write(TAG_WATERMARK);
                target.writeLong(((StreamElement) value).asWatermark().getTimestamp());
            } else if (((StreamElement) value).isWatermarkStatus()) {
                target.write(TAG_STREAM_STATUS);
                target.writeInt(((StreamElement) value).asWatermarkStatus().getStatus());
            } else if (((StreamElement) value).isLatencyMarker()) {
                target.write(TAG_LATENCY_MARKER);
                target.writeLong(((StreamElement) value).asLatencyMarker().getMarkedTime());
                target.writeLong(
                        ((LatencyMarker) value).asLatencyMarker().getOperatorId().getLowerPart());
                target.writeLong(
                        ((LatencyMarker) value).asLatencyMarker().getOperatorId().getUpperPart());
                target.writeInt(((LatencyMarker) value).asLatencyMarker().getSubtaskIndex());
            } else {
                throw new RuntimeException();
            }
        } else {
            Class<?> watermarkClazz = value.getClass();
            if (value instanceof ProcessWatermarkWrapper) {
                watermarkClazz = ((ProcessWatermarkWrapper) value).getProcessWatermark().getClass();
            }
            GeneralizedWatermarkTypeSerializer generalizedWatermarkSerializer =
                    generalizedWatermarkSerializers.get(watermarkClazz);
            if (generalizedWatermarkSerializer == null) {
                throw new RuntimeException(
                        "Can not serialize generalized watermark for "
                                + value.getClass().getSimpleName());
            }
            target.write(TAG_GENERALIZED_WATERMARK);
            target.writeUTF(watermarkClazz.getName());
            generalizedWatermarkSerializer.serialize(value, target);
        }
    }

    @Override
    public GeneralizedStreamElement deserialize(DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            return new StreamRecord<T>(recordSerializer.deserialize(source), timestamp);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            return new StreamRecord<T>(recordSerializer.deserialize(source));
        } else if (tag == TAG_WATERMARK) {
            return new Watermark(source.readLong());
        } else if (tag == TAG_STREAM_STATUS) {
            return new WatermarkStatus(source.readInt());
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else if (tag == TAG_GENERALIZED_WATERMARK) {
            String className = source.readUTF();
            TypeSerializer<GeneralizedWatermark> generalizedWatermarkTypeSerializer;
            try {
                generalizedWatermarkTypeSerializer =
                        generalizedWatermarkSerializers.get(Class.forName(className));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (generalizedWatermarkTypeSerializer == null) {
                throw new RuntimeException(
                        "Can not serialize generalized watermark for " + className);
            }
            return generalizedWatermarkTypeSerializer.deserialize(source);
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public GeneralizedStreamElement deserialize(
            GeneralizedStreamElement reuse, DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            T value = recordSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = ((StreamElement) reuse).asRecord();
            reuseRecord.replace(value, timestamp);
            return reuseRecord;
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            T value = recordSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = ((StreamElement) reuse).asRecord();
            reuseRecord.replace(value);
            return reuseRecord;
        } else if (tag == TAG_WATERMARK) {
            return new Watermark(source.readLong());
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else if (tag == TAG_GENERALIZED_WATERMARK) {
            String className = source.readUTF();
            TypeSerializer<GeneralizedWatermark> generalizedWatermarkTypeSerializer;
            try {
                generalizedWatermarkTypeSerializer =
                        generalizedWatermarkSerializers.get(Class.forName(className));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (generalizedWatermarkTypeSerializer == null) {
                throw new RuntimeException(
                        "Can not serialize generalized watermark for " + className);
            }
            return generalizedWatermarkTypeSerializer.deserialize(source);
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int tag = source.readByte();
        target.write(tag);

        if (tag == TAG_REC_WITH_TIMESTAMP) {
            // move timestamp
            target.writeLong(source.readLong());
            recordSerializer.copy(source, target);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            recordSerializer.copy(source, target);
        } else if (tag == TAG_WATERMARK) {
            target.writeLong(source.readLong());
        } else if (tag == TAG_STREAM_STATUS) {
            target.writeInt(source.readInt());
        } else if (tag == TAG_LATENCY_MARKER) {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        } else if (tag == TAG_GENERALIZED_WATERMARK) {
            String className = source.readUTF();
            // move className
            target.writeUTF(className);

            TypeSerializer<GeneralizedWatermark> generalizedWatermarkTypeSerializer;
            try {
                generalizedWatermarkTypeSerializer =
                        generalizedWatermarkSerializers.get(Class.forName(className));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (generalizedWatermarkTypeSerializer == null) {
                throw new RuntimeException(
                        "Can not serialize generalized watermark for " + className);
            }
            generalizedWatermarkTypeSerializer.copy(source, target);
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeneralizedStreamElementSerializer<?> that = (GeneralizedStreamElementSerializer<?>) o;
        return Objects.equals(generalizedWatermarkSerializers, that.generalizedWatermarkSerializers)
                && Objects.equals(recordSerializer, that.recordSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generalizedWatermarkSerializers, recordSerializer);
    }

    public TypeSerializer<T> getContainedTypeSerializer() {
        return this.recordSerializer;
    }

    @Override
    public TypeSerializerSnapshot<GeneralizedStreamElement> snapshotConfiguration() {
        return null;
    }

    /** Configuration snapshot specific to the {@link GeneralizedStreamElement}. */
    public static final class GeneralizedStreamElementSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<
                    GeneralizedStreamElement, GeneralizedStreamElementSerializer<T>> {

        private static final int VERSION = 1;

        private final Map<Class<?>, GeneralizedWatermarkTypeSerializer>
                generalizedWatermarkSerializers;

        @SuppressWarnings("WeakerAccess")
        public GeneralizedStreamElementSerializerSnapshot(
                Map<Class<?>, GeneralizedWatermarkTypeSerializer> generalizedWatermarkSerializers) {
            super(GeneralizedStreamElementSerializer.class);
            this.generalizedWatermarkSerializers = generalizedWatermarkSerializers;
        }

        GeneralizedStreamElementSerializerSnapshot(
                GeneralizedStreamElementSerializer<T> serializerInstance,
                Map<Class<?>, GeneralizedWatermarkTypeSerializer> generalizedWatermarkSerializers) {
            super(serializerInstance);
            this.generalizedWatermarkSerializers = generalizedWatermarkSerializers;
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                GeneralizedStreamElementSerializer<T> outerSerializer) {
            return new TypeSerializer[] {outerSerializer.getContainedTypeSerializer()};
        }

        @Override
        protected GeneralizedStreamElementSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> casted = (TypeSerializer<T>) nestedSerializers[0];

            return new GeneralizedStreamElementSerializer<>(
                    casted, generalizedWatermarkSerializers);
        }
    }
}
