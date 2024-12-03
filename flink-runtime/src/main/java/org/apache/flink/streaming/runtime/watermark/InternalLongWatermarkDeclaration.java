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

package org.apache.flink.streaming.runtime.watermark;

import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;

/**
 * The {@link InternalLongWatermarkDeclaration} class implements the {@link
 * AbstractInternalWatermarkDeclaration} interface and provides additional functionality specific to
 * long-type watermarks.
 */
public class InternalLongWatermarkDeclaration extends AbstractInternalWatermarkDeclaration<Long> {

    public InternalLongWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction) {
        this(identifier, combinationPolicyForChannel, defaultHandlingStrategyForFunction, false);
    }

    public InternalLongWatermarkDeclaration(LongWatermarkDeclaration declaration) {
        this(
                declaration.getIdentifier(),
                declaration.getCombinationPolicy(),
                declaration.getDefaultHandlingStrategy(),
                (declaration instanceof Alignable) && ((Alignable) declaration).isAligned());
    }

    public InternalLongWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction,
            boolean isAligned) {
        super(
                identifier,
                combinationPolicyForChannel,
                defaultHandlingStrategyForFunction,
                isAligned);
    }

    /** Creates a new {@code LongWatermark} with the specified value. */
    @Override
    public LongWatermark newWatermark(Long val) {
        return new LongWatermark(val, this.getIdentifier());
    }

    @Override
    public WatermarkCombiner watermarkCombiner(int numberOfInputChannels, Runnable gateResumer) {
        if (isAligned) {
            return new AlignedWatermarkCombiner(numberOfInputChannels, gateResumer);
        } else {
            return new LongWatermarkCombiner(getCombinationPolicy(), numberOfInputChannels);
        }
    }
}
