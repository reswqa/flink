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

package org.apache.flink.processfunction;

import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermarkWrapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.state.StateDeclaration.ListStateDeclaration.RedistributionStrategy;
import org.apache.flink.processfunction.state.ListStateDeclarationImpl;
import org.apache.flink.processfunction.state.MapStateDeclarationImpl;
import org.apache.flink.processfunction.state.StateDeclarationConverter;
import org.apache.flink.processfunction.state.ValueStateDeclarationImpl;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** The default implementations of {@link RuntimeContext}. */
public class DefaultRuntimeContext implements RuntimeContext {
    private final Set<StateDeclaration> usedStates;

    private final OperatorStateStore operatorStateStore;

    private final StreamingRuntimeContext streamingRuntimeContext;

    private final Supplier<Object> currentKeySupplier;

    private final Output<?> watermarkEmitter;

    private final Consumer<Long> processingTimerRegister;

    /**
     * {@link #getCurrentKey()} will return this if it is not null, otherwise return the key from
     * {@link #currentKeySupplier}.
     */
    @Nullable private Object overwriteCurrentKey;

    public DefaultRuntimeContext(
            Set<StateDeclaration> usedStates,
            OperatorStateStore operatorStateStore,
            StreamingRuntimeContext streamingRuntimeContext,
            Supplier<Object> currentKeySupplier,
            Consumer<Long> processingTimerRegister,
            Output<?> watermarkEmitter) {
        this.usedStates = usedStates;
        this.operatorStateStore = operatorStateStore;
        this.streamingRuntimeContext = streamingRuntimeContext;
        this.currentKeySupplier = currentKeySupplier;
        this.watermarkEmitter = watermarkEmitter;
        this.processingTimerRegister = processingTimerRegister;
    }

    @Override
    public <T> Optional<ListState<T>> getState(
            StateDeclaration.ListStateDeclaration stateDeclaration) throws Exception {
        if (!usedStates.contains(stateDeclaration)) {
            return Optional.empty();
        }

        ListStateDescriptor<T> listStateDescriptor =
                StateDeclarationConverter.getListStateDescriptor(
                        (ListStateDeclarationImpl<T>) stateDeclaration);

        StateDeclaration.RedistributionMode redistributionMode =
                stateDeclaration.getRedistributionMode();
        if (redistributionMode == StateDeclaration.RedistributionMode.REDISTRIBUTABLE) {
            RedistributionStrategy redistributionStrategy =
                    stateDeclaration.getRedistributionStrategy();
            if (redistributionStrategy == RedistributionStrategy.UNION) {
                // union list state
                return Optional.ofNullable(
                        operatorStateStore.getUnionListState(listStateDescriptor));
            } else {
                // split list state
                return Optional.ofNullable(operatorStateStore.getListState(listStateDescriptor));
            }
        } else if (redistributionMode == StateDeclaration.RedistributionMode.NONE) {
            try {
                return Optional.ofNullable(
                        streamingRuntimeContext.getListState(listStateDescriptor));
            } catch (Exception e) {
                return Optional.empty();
            }
        } else {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + redistributionMode.name()
                            + " is not supported for list state.");
        }
    }

    @Override
    public <T> Optional<ValueState<T>> getState(
            StateDeclaration.ValueStateDeclaration stateDeclaration) throws Exception {
        if (!usedStates.contains(stateDeclaration)) {
            return Optional.empty();
        }

        ValueStateDescriptor<T> valueStateDescriptor =
                StateDeclarationConverter.getValueStateDescriptor(
                        (ValueStateDeclarationImpl<T>) stateDeclaration);
        try {
            return Optional.ofNullable(streamingRuntimeContext.getState(valueStateDescriptor));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public void setCurrentKey(Object key) {
        overwriteCurrentKey = Preconditions.checkNotNull(key);
    }

    public void resetCurrentKey() {
        overwriteCurrentKey = null;
    }

    @Override
    public <K, V> Optional<MapState<K, V>> getState(
            StateDeclaration.MapStateDeclaration stateDeclaration) throws Exception {
        if (!usedStates.contains(stateDeclaration)) {
            return Optional.empty();
        }

        MapStateDescriptor<K, V> mapStateDescriptor =
                StateDeclarationConverter.getMapStateDescriptor(
                        (MapStateDeclarationImpl<K, V>) stateDeclaration);
        StateDeclaration.RedistributionMode redistributionMode =
                stateDeclaration.getRedistributionMode();
        if (redistributionMode == StateDeclaration.RedistributionMode.IDENTICAL) {
            try {
                return Optional.ofNullable(
                        operatorStateStore.getBroadcastState(mapStateDescriptor));
            } catch (Exception e) {
                return Optional.empty();
            }
        } else if (redistributionMode == StateDeclaration.RedistributionMode.NONE) {
            try {
                return Optional.ofNullable(streamingRuntimeContext.getMapState(mapStateDescriptor));
            } catch (Exception e) {
                return Optional.empty();
            }
        } else {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + redistributionMode.name()
                            + " is not supported for map state.");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> Optional<K> getCurrentKey() {
        try {
            if (overwriteCurrentKey != null) {
                return Optional.of((K) overwriteCurrentKey);
            }
            return Optional.ofNullable((K) currentKeySupplier.get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public <T extends ProcessWatermark<T>> void emitWatermark(ProcessWatermark<T> watermark) {
        watermarkEmitter.emitWatermark(new ProcessWatermarkWrapper(watermark));
    }

    @Override
    public void registerProcessingTimer(long timestamp) {
        processingTimerRegister.accept(timestamp);
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return streamingRuntimeContext.getJobType() == JobType.STREAMING
                ? ExecutionMode.STREAMING
                : ExecutionMode.BATCH;
    }
}
