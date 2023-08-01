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

package org.apache.flink.processfunction.windows.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.SerializerContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.processfunction.api.windowing.utils.TaggedUnion;

import java.io.IOException;

public class Unions {
    @Internal
    public static class UnionTypeInfo<T1, T2> extends TypeInformation<TaggedUnion<T1, T2>> {
        private static final long serialVersionUID = 1L;

        private final TypeInformation<T1> oneType;
        private final TypeInformation<T2> twoType;

        public UnionTypeInfo(TypeInformation<T1> oneType, TypeInformation<T2> twoType) {
            this.oneType = oneType;
            this.twoType = twoType;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public int getTotalFields() {
            return 2;
        }

        @Override
        @SuppressWarnings("unchecked, rawtypes")
        public Class<TaggedUnion<T1, T2>> getTypeClass() {
            return (Class) TaggedUnion.class;
        }

        @Override
        public boolean isKeyType() {
            return true;
        }

        @Override
        public TypeSerializer<TaggedUnion<T1, T2>> createSerializer(
                SerializerContext serializerContext) {
            return new UnionSerializer<>(
                    oneType.createSerializer(serializerContext),
                    twoType.createSerializer(serializerContext));
        }

        @Override
        public String toString() {
            return "TaggedUnion<" + oneType + ", " + twoType + ">";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof UnionTypeInfo) {
                @SuppressWarnings("unchecked")
                UnionTypeInfo<T1, T2> unionTypeInfo = (UnionTypeInfo<T1, T2>) obj;

                return unionTypeInfo.canEqual(this)
                        && oneType.equals(unionTypeInfo.oneType)
                        && twoType.equals(unionTypeInfo.twoType);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 31 * oneType.hashCode() + twoType.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof UnionTypeInfo;
        }
    }

    /** {@link TypeSerializer} for {@link TaggedUnion}. */
    @Internal
    public static class UnionSerializer<T1, T2> extends TypeSerializer<TaggedUnion<T1, T2>> {
        private static final long serialVersionUID = 1L;

        private final TypeSerializer<T1> oneSerializer;
        private final TypeSerializer<T2> twoSerializer;

        public UnionSerializer(TypeSerializer<T1> oneSerializer, TypeSerializer<T2> twoSerializer) {
            this.oneSerializer = oneSerializer;
            this.twoSerializer = twoSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TaggedUnion<T1, T2>> duplicate() {
            TypeSerializer<T1> duplicateOne = oneSerializer.duplicate();
            TypeSerializer<T2> duplicateTwo = twoSerializer.duplicate();

            // compare reference of nested serializers, if same instances returned, we can reuse
            // this instance as well
            if (duplicateOne != oneSerializer || duplicateTwo != twoSerializer) {
                return new UnionSerializer<>(duplicateOne, duplicateTwo);
            } else {
                return this;
            }
        }

        @Override
        public TaggedUnion<T1, T2> createInstance() {
            // we arbitrarily always create instance of one
            return TaggedUnion.one(oneSerializer.createInstance());
        }

        @Override
        public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from) {
            if (from.isOne()) {
                return TaggedUnion.one(oneSerializer.copy(from.getOne()));
            } else {
                return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
            }
        }

        @Override
        public TaggedUnion<T1, T2> copy(TaggedUnion<T1, T2> from, TaggedUnion<T1, T2> reuse) {
            if (from.isOne()) {
                return TaggedUnion.one(oneSerializer.copy(from.getOne()));
            } else {
                return TaggedUnion.two(twoSerializer.copy(from.getTwo()));
            }
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(TaggedUnion<T1, T2> record, DataOutputView target)
                throws IOException {
            if (record.isOne()) {
                target.writeByte(1);
                oneSerializer.serialize(record.getOne(), target);
            } else {
                target.writeByte(2);
                twoSerializer.serialize(record.getTwo(), target);
            }
        }

        @Override
        public TaggedUnion<T1, T2> deserialize(DataInputView source) throws IOException {
            byte tag = source.readByte();
            if (tag == 1) {
                return TaggedUnion.one(oneSerializer.deserialize(source));
            } else {
                return TaggedUnion.two(twoSerializer.deserialize(source));
            }
        }

        @Override
        public TaggedUnion<T1, T2> deserialize(TaggedUnion<T1, T2> reuse, DataInputView source)
                throws IOException {
            byte tag = source.readByte();
            if (tag == 1) {
                return TaggedUnion.one(oneSerializer.deserialize(source));
            } else {
                return TaggedUnion.two(twoSerializer.deserialize(source));
            }
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            byte tag = source.readByte();
            target.writeByte(tag);
            if (tag == 1) {
                oneSerializer.copy(source, target);
            } else {
                twoSerializer.copy(source, target);
            }
        }

        @Override
        public int hashCode() {
            return 31 * oneSerializer.hashCode() + twoSerializer.hashCode();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean equals(Object obj) {
            if (obj instanceof UnionSerializer) {
                UnionSerializer<T1, T2> other = (UnionSerializer<T1, T2>) obj;

                return oneSerializer.equals(other.oneSerializer)
                        && twoSerializer.equals(other.twoSerializer);
            } else {
                return false;
            }
        }

        @Override
        public TypeSerializerSnapshot<TaggedUnion<T1, T2>> snapshotConfiguration() {
            return new UnionSerializerSnapshot<>(this);
        }
    }

    /** The {@link TypeSerializerSnapshot} for the {@link UnionSerializer}. */
    public static class UnionSerializerSnapshot<T1, T2>
            extends CompositeTypeSerializerSnapshot<TaggedUnion<T1, T2>, UnionSerializer<T1, T2>> {

        private static final int VERSION = 2;

        @SuppressWarnings("WeakerAccess")
        public UnionSerializerSnapshot() {
            super(UnionSerializer.class);
        }

        UnionSerializerSnapshot(UnionSerializer<T1, T2> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                UnionSerializer<T1, T2> outerSerializer) {
            return new TypeSerializer[] {
                outerSerializer.oneSerializer, outerSerializer.twoSerializer
            };
        }

        @SuppressWarnings("unchecked")
        @Override
        protected UnionSerializer<T1, T2> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new UnionSerializer<>(
                    (TypeSerializer<T1>) nestedSerializers[0],
                    (TypeSerializer<T2>) nestedSerializers[1]);
        }
    }
}
