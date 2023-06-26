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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.state.ListStateDeclarationImpl;
import org.apache.flink.processfunction.state.StateDeclarationConverter;
import org.apache.flink.processfunction.state.ValueStateDeclarationImpl;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/** The default implementations of {@link RuntimeContext}. */
public class DefaultRuntimeContext implements RuntimeContext {
    private final Set<StateDeclaration> usedStates;

    private final OperatorStateStore operatorStateStore;

    private final StreamingRuntimeContext streamingRuntimeContext;

    private final Supplier<Object> currentKeySupplier;

    public DefaultRuntimeContext(
            Set<StateDeclaration> usedStates,
            OperatorStateStore operatorStateStore,
            StreamingRuntimeContext streamingRuntimeContext,
            Supplier<Object> currentKeySupplier) {
        this.usedStates = usedStates;
        this.operatorStateStore = operatorStateStore;
        this.streamingRuntimeContext = streamingRuntimeContext;
        this.currentKeySupplier = currentKeySupplier;
    }

    @Override
    public <T> Optional<ListState<T>> getState(
            StateDeclaration.ListStateDeclaration<T> stateDeclaration) throws Exception {
        if (!usedStates.contains(stateDeclaration)) {
            return Optional.empty();
        }

        ListStateDescriptor<T> listStateDescriptor =
                StateDeclarationConverter.getListStateDescriptor(
                        (ListStateDeclarationImpl<T>) stateDeclaration);
        StateDeclaration.Scope scope = stateDeclaration.getScope();
        if (scope == StateDeclaration.Scope.OPERATOR) {
            return Optional.ofNullable(operatorStateStore.getListState(listStateDescriptor));
        } else if (scope == StateDeclaration.Scope.KEYED) {
            try {
                return Optional.ofNullable(
                        streamingRuntimeContext.getListState(listStateDescriptor));
            } catch (Exception e) {
                return Optional.empty();
            }
        } else {
            throw new UnsupportedOperationException(
                    "Scope type " + scope.name() + " is not supported.");
        }
    }

    @Override
    public <T> Optional<ValueState<T>> getState(
            StateDeclaration.ValueStateDeclaration<T> stateDeclaration) throws Exception {
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

    @Override
    @SuppressWarnings("unchecked")
    public <K> Optional<K> getCurrentKey() {
        try {
            return Optional.ofNullable((K) currentKeySupplier.get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return streamingRuntimeContext.getJobType() == JobType.STREAMING
                ? ExecutionMode.STREAMING
                : ExecutionMode.BATCH;
    }
}
