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

package org.apache.flink.api.common.watermark;

/**
 * The {@link BoolWatermarkDeclaration} class implements the {@link WatermarkDeclaration} interface
 * and provides additional functionality specific to boolean-type watermarks. It includes methods
 * for obtaining combination semantics and creating new bool watermarks.
 */
public class BoolWatermarkDeclaration implements WatermarkDeclaration {

    private final String identifier;

    private final WatermarkCombinationPolicy combinationPolicyForChannel;

    private final WatermarkHandlingStrategy defaultHandlingStrategyForFunction;

    public BoolWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction) {
        this.identifier = identifier;
        this.combinationPolicyForChannel = combinationPolicyForChannel;
        this.defaultHandlingStrategyForFunction = defaultHandlingStrategyForFunction;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    /** Creates a new {@link BoolWatermark} with the specified boolean value. */
    public BoolWatermark newWatermark(boolean val) {
        return new BoolWatermark(val, identifier);
    }

    public WatermarkCombinationPolicy getCombinationPolicyForChannel() {
        return combinationPolicyForChannel;
    }

    public WatermarkHandlingStrategy getDefaultHandlingStrategyForFunction() {
        return defaultHandlingStrategyForFunction;
    }
}
