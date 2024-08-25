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

package org.apache.flink.streaming.util.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;

/**
 * The {@link AbstractInternalWatermarkDeclaration} class implements the {@code
 * WatermarkDeclaration} interface and provides some internal-oriented methods, such as
 * getting/setting the align flag and creating the {@link WatermarkCombiner}.
 */
@Internal
public abstract class AbstractInternalWatermarkDeclaration<T> implements WatermarkDeclaration {

    protected final String identifier;

    protected final WatermarkCombinationPolicy combinationPolicyForChannel;

    protected final WatermarkHandlingStrategy defaultHandlingStrategyForFunction;

    protected final boolean isAligned;

    public AbstractInternalWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction,
            boolean isAligned) {
        this.identifier = identifier;
        this.combinationPolicyForChannel = combinationPolicyForChannel;
        this.defaultHandlingStrategyForFunction = defaultHandlingStrategyForFunction;
        this.isAligned = isAligned;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    /** Creates a new {@link Watermark} with the specified value. */
    public abstract Watermark newWatermark(T val);

    public WatermarkCombinationPolicy getCombinationPolicyForChannel() {
        return combinationPolicyForChannel;
    }

    public WatermarkHandlingStrategy getDefaultHandlingStrategyForFunction() {
        return defaultHandlingStrategyForFunction;
    }

    public boolean isAligned() {
        return isAligned;
    }

    /** Creates a new {@link WatermarkCombiner} instance. */
    public abstract WatermarkCombiner watermarkCombiner(
            int numberOfInputChannels, Runnable gateResumer);

    /**
     * Converts a user-oriented {@link WatermarkDeclaration} to an internal-oriented {@link
     * AbstractInternalWatermarkDeclaration}.
     */
    public static AbstractInternalWatermarkDeclaration<?> from(
            WatermarkDeclaration watermarkDeclaration) {
        if (watermarkDeclaration instanceof BoolWatermarkDeclaration) {
            return new InternalBoolWatermarkDeclaration(
                    (BoolWatermarkDeclaration) watermarkDeclaration);
        } else if (watermarkDeclaration instanceof LongWatermarkDeclaration) {
            return new InternalLongWatermarkDeclaration(
                    (LongWatermarkDeclaration) watermarkDeclaration);
        } else {
            throw new IllegalArgumentException(
                    "Unknown watermark declaration type: " + watermarkDeclaration.getClass());
        }
    }
}
