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
import org.apache.flink.api.common.typeutils.ProcessWatermarkWrapperTypeSerializer;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.Serializable;

public class GeneralizedWatermarkDeclaration implements Serializable {
    private final Class<?> watermarkClazz;

    private final SerializableSupplier<GeneralizedWatermark<?>> minWatermarkSupplier;

    private final SerializableSupplier<GeneralizedWatermark<?>> maxWatermarkSupplier;

    private final WatermarkAlignmentStrategy alignmentStrategy;

    private final GeneralizedWatermarkTypeSerializer<?> watermarkTypeSerializer;

    public GeneralizedWatermarkDeclaration(
            Class<?> watermarkClazz,
            SerializableSupplier<GeneralizedWatermark<?>> minWatermarkSupplier,
            SerializableSupplier<GeneralizedWatermark<?>> maxWatermarkSupplier,
            WatermarkAlignmentStrategy alignmentStrategy,
            GeneralizedWatermarkTypeSerializer<?> watermarkTypeSerializer) {
        this.watermarkClazz = watermarkClazz;
        this.minWatermarkSupplier = minWatermarkSupplier;
        this.maxWatermarkSupplier = maxWatermarkSupplier;
        this.alignmentStrategy = alignmentStrategy;
        this.watermarkTypeSerializer = watermarkTypeSerializer;
    }

    public static GeneralizedWatermarkDeclaration fromProcessWatermark(
            ProcessWatermarkDeclaration processWatermarkDeclaration) {
        return new GeneralizedWatermarkDeclaration(
                processWatermarkDeclaration.getWatermarkClazz(),
                () ->
                        new ProcessWatermarkWrapper(
                                processWatermarkDeclaration.getMinWatermarkSupplier().get()),
                () ->
                        new ProcessWatermarkWrapper(
                                processWatermarkDeclaration.getMaxWatermarkSupplier().get()),
                processWatermarkDeclaration.getAlignmentStrategy(),
                new ProcessWatermarkWrapperTypeSerializer(
                        processWatermarkDeclaration.getWatermarkTypeSerializer()));
    }

    public WatermarkAlignmentStrategy getAlignmentStrategy() {
        return alignmentStrategy;
    }

    public Class<?> getWatermarkClazz() {
        return watermarkClazz;
    }

    public GeneralizedWatermarkTypeSerializer getWatermarkTypeSerializer() {
        return watermarkTypeSerializer;
    }

    public SerializableSupplier<GeneralizedWatermark<?>> getMinWatermarkSupplier() {
        return minWatermarkSupplier;
    }

    public SerializableSupplier<GeneralizedWatermark<?>> getMaxWatermarkSupplier() {
        return maxWatermarkSupplier;
    }
}
