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
import org.apache.flink.api.common.typeinfo.TypeSerializer;
import org.apache.flink.api.common.typeinfo.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.datastream.api.extension.window.window.GlobalWindow;

import java.io.IOException;

public class GlobalWindowImpl implements UnboundedWindow, GlobalWindow {
    private static final GlobalWindowImpl INSTANCE = new GlobalWindowImpl();

    private GlobalWindowImpl() {}

    public static GlobalWindowImpl get() {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass());
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "GlobalWindow";
    }

    /** A {@link TypeSerializer} for {@link GlobalWindowImpl}. */
    public static class Serializer extends TypeSerializerSingleton<GlobalWindowImpl> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public GlobalWindowImpl createInstance() {
            return GlobalWindowImpl.INSTANCE;
        }

        @Override
        public GlobalWindowImpl copy(GlobalWindowImpl from) {
            return from;
        }

        @Override
        public GlobalWindowImpl copy(GlobalWindowImpl from, GlobalWindowImpl reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return Byte.BYTES;
        }

        @Override
        public void serialize(GlobalWindowImpl record, DataOutputView target) throws IOException {
            target.writeByte(0);
        }

        @Override
        public GlobalWindowImpl deserialize(DataInputView source) throws IOException {
            source.readByte();
            return GlobalWindowImpl.INSTANCE;
        }

        @Override
        public GlobalWindowImpl deserialize(GlobalWindowImpl reuse, DataInputView source)
                throws IOException {
            source.readByte();
            return GlobalWindowImpl.INSTANCE;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            source.readByte();
            target.writeByte(0);
        }

        // ------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<GlobalWindowImpl> snapshotConfiguration() {
            return new GlobalWindowImpl.Serializer.GlobalWindowSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class GlobalWindowSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<GlobalWindowImpl> {

            public GlobalWindowSerializerSnapshot() {
                super(GlobalWindowImpl.Serializer::new);
            }
        }
    }
}
