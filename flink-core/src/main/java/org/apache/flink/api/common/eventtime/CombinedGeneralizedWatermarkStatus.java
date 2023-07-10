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

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CombinedGeneralizedWatermarkStatus} combines the watermark (and idleness) updates of
 * multiple inputs into one combined watermark.
 */
@Internal
final class CombinedGeneralizedWatermarkStatus {

    /** List of all watermark outputs, for efficient access. */
    private final List<PartialWatermark> partialWatermarks = new ArrayList<>();

    /** The combined watermark over the per-output watermarks. */
    private GeneralizedWatermark combinedWatermark;

    private boolean idle = false;

    private final GeneralizedWatermarkDeclaration generalizedWatermarkDeclaration;

    public CombinedGeneralizedWatermarkStatus(
            GeneralizedWatermarkDeclaration generalizedWatermarkDeclaration) {
        this.generalizedWatermarkDeclaration = generalizedWatermarkDeclaration;
        this.combinedWatermark = generalizedWatermarkDeclaration.getMinWatermarkSupplier().get();
    }

    public GeneralizedWatermark getCombinedWatermark() {
        return combinedWatermark;
    }

    public boolean isIdle() {
        return idle;
    }

    public boolean remove(PartialWatermark o) {
        return partialWatermarks.remove(o);
    }

    public void add(PartialWatermark element) {
        partialWatermarks.add(element);
    }

    /**
     * Checks whether we need to update the combined watermark.
     *
     * <p><b>NOTE:</b>It can update {@link #isIdle()} status.
     *
     * @return true, if the combined watermark changed
     */
    public boolean updateCombinedWatermark() {
        GeneralizedWatermark minimumOverAllOutputs =
                generalizedWatermarkDeclaration.getMaxWatermarkSupplier().get();

        // if we don't have any outputs minimumOverAllOutputs is not valid, it's still
        // at its initial state, and we must not emit that
        if (partialWatermarks.isEmpty()) {
            return false;
        }

        boolean allIdle = true;
        for (PartialWatermark partialWatermark : partialWatermarks) {
            if (!partialWatermark.isIdle()) {
                if (partialWatermark.getWatermark().compareTo(minimumOverAllOutputs) < 0) {
                    minimumOverAllOutputs = partialWatermark.getWatermark();
                }
                allIdle = false;
            }
        }

        this.idle = allIdle;

        if (!allIdle && minimumOverAllOutputs.compareTo(combinedWatermark) > 0) {
            combinedWatermark = minimumOverAllOutputs;
            return true;
        }

        return false;
    }

    /** Per-output watermark state. */
    static class PartialWatermark {
        private GeneralizedWatermark watermark;
        private boolean idle = false;

        public PartialWatermark(GeneralizedWatermark minWatermark) {
            this.watermark = minWatermark;
        }

        /**
         * Returns the current watermark timestamp. This will throw {@link IllegalStateException} if
         * the output is currently idle.
         */
        private GeneralizedWatermark getWatermark() {
            checkState(!idle, "Output is idle.");
            return watermark;
        }

        /**
         * Returns true if the watermark was advanced, that is if the new watermark is larger than
         * the previous one.
         *
         * <p>Setting a watermark will clear the idleness flag.
         */
        public boolean setWatermark(GeneralizedWatermark watermark) {
            this.idle = false;
            final boolean updated = watermark.compareTo(this.watermark) > 0;
            if (updated) {
                this.watermark = watermark;
            }
            return updated;
        }

        private boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
        }
    }
}
