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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public class ProcessWatermarkDeclaration implements Serializable {
    private final Class<?> watermarkClazz;

    // This min & max watermark is used to align watermark by min-max mode, we should reconsider
    // this after support more alignment mode.
    private final SerializableSupplier<ProcessWatermark> minWatermarkSupplier;

    private final SerializableSupplier<ProcessWatermark> maxWatermarkSupplier;

    private final WatermarkAlignmentStrategy alignmentStrategy;

    private final TypeSerializer<? extends ProcessWatermark> watermarkTypeSerializer;

    public Class<?> getWatermarkClazz() {
        return watermarkClazz;
    }

    public ProcessWatermarkDeclaration(
            Class<? extends ProcessWatermark<?>> watermarkClazz,
            SerializableSupplier<ProcessWatermark> minWatermarkSupplier,
            SerializableSupplier<ProcessWatermark> maxWatermarkSupplier,
            WatermarkAlignmentStrategy alignmentStrategy,
            TypeSerializer<? extends ProcessWatermark> watermarkTypeSerializer) {
        this.watermarkClazz = watermarkClazz;
        this.minWatermarkSupplier = minWatermarkSupplier;
        this.maxWatermarkSupplier = maxWatermarkSupplier;
        this.alignmentStrategy = alignmentStrategy;
        this.watermarkTypeSerializer = watermarkTypeSerializer;
    }

    public SerializableSupplier<ProcessWatermark> getMinWatermarkSupplier() {
        return minWatermarkSupplier;
    }

    public SerializableSupplier<ProcessWatermark> getMaxWatermarkSupplier() {
        return maxWatermarkSupplier;
    }

    @Nullable
    public TypeSerializer getWatermarkTypeSerializer() {
        return watermarkTypeSerializer;
    }

    public WatermarkAlignmentStrategy getAlignmentStrategy() {
        return alignmentStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessWatermarkDeclaration that = (ProcessWatermarkDeclaration) o;
        return Objects.equals(getWatermarkClazz(), that.getWatermarkClazz())
                && Objects.equals(getMinWatermarkSupplier(), that.getMinWatermarkSupplier())
                && Objects.equals(getAlignmentStrategy(), that.getAlignmentStrategy())
                && Objects.equals(getWatermarkTypeSerializer(), that.getWatermarkTypeSerializer());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getWatermarkClazz(),
                getMinWatermarkSupplier(),
                getAlignmentStrategy(),
                getWatermarkTypeSerializer());
    }
}
