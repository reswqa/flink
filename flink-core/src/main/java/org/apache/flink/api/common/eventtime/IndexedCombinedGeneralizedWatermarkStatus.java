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

import org.apache.flink.annotation.Internal;

import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Represents combined value and status of a watermark for a set number of input partial watermarks.
 */
@Internal
public final class IndexedCombinedGeneralizedWatermarkStatus {
    private final CombinedGeneralizedWatermarkStatus combinedWatermarkStatus;
    private final CombinedGeneralizedWatermarkStatus.PartialWatermark[] partialWatermarks;

    private IndexedCombinedGeneralizedWatermarkStatus(
            GeneralizedWatermarkDeclaration generalizedWatermarkDeclaration,
            CombinedGeneralizedWatermarkStatus combinedWatermarkStatus,
            CombinedGeneralizedWatermarkStatus.PartialWatermark[] partialWatermarks) {
        this.combinedWatermarkStatus = combinedWatermarkStatus;
        this.partialWatermarks = partialWatermarks;
    }

    public static IndexedCombinedGeneralizedWatermarkStatus forInputsCount(
            GeneralizedWatermarkDeclaration generalizedWatermarkDeclaration, int inputsCount) {
        CombinedGeneralizedWatermarkStatus.PartialWatermark[] partialWatermarks =
                IntStream.range(0, inputsCount)
                        .mapToObj(
                                i ->
                                        new CombinedGeneralizedWatermarkStatus.PartialWatermark(
                                                generalizedWatermarkDeclaration
                                                        .getMinWatermarkSupplier()
                                                        .get()))
                        .toArray(CombinedGeneralizedWatermarkStatus.PartialWatermark[]::new);
        CombinedGeneralizedWatermarkStatus combinedWatermarkStatus =
                new CombinedGeneralizedWatermarkStatus(generalizedWatermarkDeclaration);
        for (CombinedGeneralizedWatermarkStatus.PartialWatermark partialWatermark :
                partialWatermarks) {
            combinedWatermarkStatus.add(partialWatermark);
        }
        return new IndexedCombinedGeneralizedWatermarkStatus(
                generalizedWatermarkDeclaration, combinedWatermarkStatus, partialWatermarks);
    }

    /**
     * Updates the value for the given partial watermark. Can update both the global idleness as
     * well as the combined watermark value.
     *
     * @return true, if the combined watermark value changed. The global idleness needs to be
     *     checked separately via {@link #isIdle()}
     */
    public boolean updateWatermark(int index, GeneralizedWatermark watermark) {
        checkArgument(index < partialWatermarks.length);
        partialWatermarks[index].setWatermark(watermark);
        return combinedWatermarkStatus.updateCombinedWatermark();
    }

    public GeneralizedWatermark getCombinedWatermark() {
        return combinedWatermarkStatus.getCombinedWatermark();
    }

    /**
     * Updates the idleness for the given partial watermark. Can update both the global idleness as
     * well as the combined watermark value.
     *
     * @return true, if the combined watermark value changed. The global idleness needs to be
     *     checked separately via {@link #isIdle()}
     */
    public boolean updateStatus(int index, boolean idle) {
        checkArgument(index < partialWatermarks.length);
        partialWatermarks[index].setIdle(idle);
        return combinedWatermarkStatus.updateCombinedWatermark();
    }

    public boolean isIdle() {
        return combinedWatermarkStatus.isIdle();
    }
}
