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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.memory.DataInputView;
import org.apache.flink.api.common.memory.DataOutputView;
import org.apache.flink.api.common.typeinfo.utils.TypeSerializer;
import org.apache.flink.api.common.typeinfo.utils.TypeSerializerSnapshot;

import java.io.IOException;
import java.util.Arrays;

/** Utilities for dealing with the {@link TypeSerializer} and the {@link TypeSerializerSnapshot}. */
public final class TypeSerializerUtils {

    /** Takes snapshots of the given serializers. */
    public static TypeSerializerSnapshot<?>[] snapshot(
            TypeSerializer<?>... originatingSerializers) {

        return Arrays.stream(originatingSerializers)
                .map(
                        (TypeSerializer<?> originatingSerializer) ->
                                originatingSerializer.snapshotConfiguration())
                .toArray(TypeSerializerSnapshot[]::new);
    }

    // ------------------------------------------------------------------------

    /** This class is not meanto to be instantiated. */
    private TypeSerializerUtils() {}

    /**
     * Writes the given snapshot to the out stream. One should always use this method to write
     * snapshots out, rather than directly calling {@link TypeSerializerSnapshot#writeSnapshot(DataOutputView)}.
     *
     * <p>The snapshot written with this method can be read via {@link
     * #readVersionedSnapshot(DataInputView, ClassLoader)}.
     */
    public static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot)
            throws IOException {
        out.writeUTF(snapshot.getClass().getName());
        out.writeInt(snapshot.getCurrentVersion());
        snapshot.writeSnapshot(out);
    }

    /**
     * Reads a snapshot from the stream, performing resolving
     *
     * <p>This method reads snapshots written by {@link #writeVersionedSnapshot(DataOutputView,
     * TypeSerializerSnapshot)}.
     */
    public static <T> TypeSerializerSnapshot<T> readVersionedSnapshot(DataInputView in, ClassLoader cl)
            throws IOException {
        final TypeSerializerSnapshot<T> snapshot =
                TypeSerializerSnapshotSerializationUtil.readAndInstantiateSnapshotClass(in, cl);

        int version = in.readInt();
        snapshot.readSnapshot(version, in, cl);
        return snapshot;
    }
}
