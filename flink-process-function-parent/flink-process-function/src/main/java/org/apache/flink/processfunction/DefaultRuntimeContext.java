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

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.StateDeclarationConverter;
import org.apache.flink.api.common.state.States;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.processfunction.api.RuntimeContext;

import java.util.Set;

/** The default implementations of {@link RuntimeContext}. */
public class DefaultRuntimeContext implements RuntimeContext {
    private final Set<States.StateDeclaration> usedStates;

    private final OperatorStateStore operatorStateStore;

    private final KeyedStateStore keyedStateStore;

    public DefaultRuntimeContext(
            Set<States.StateDeclaration> usedStates,
            OperatorStateStore operatorStateStore,
            KeyedStateStore keyedStateStore) {
        this.usedStates = usedStates;
        this.operatorStateStore = operatorStateStore;
        this.keyedStateStore = keyedStateStore;
    }

    @Override
    public <T> ListState<T> getState(States.ListStateDeclaration<T> stateDeclaration)
            throws Exception {
        if (!usedStates.contains(stateDeclaration)) {
            throw new IllegalArgumentException("This state is not registered.");
        }
        ListStateDescriptor<T> listStateDescriptor =
                StateDeclarationConverter.getListStateDescriptor(stateDeclaration);
        return operatorStateStore.getListState(listStateDescriptor);
    }

    @Override
    public <T> ValueState<T> getState(States.ValueStateDeclaration<T> stateDeclaration)
            throws Exception {
        if (!usedStates.contains(stateDeclaration)) {
            throw new IllegalArgumentException("This state is not registered.");
        }

        ValueStateDescriptor<T> valueStateDescriptor =
                StateDeclarationConverter.getValueStateDescriptor(stateDeclaration);
        return keyedStateStore == null ? null : keyedStateStore.getState(valueStateDescriptor);
    }
}
