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

package org.apache.flink.processfunction.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDeclarationConverter;
import org.apache.flink.api.common.state.States;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.function.Consumer;

/** Operator for {@link TwoInputStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class KeyedTwoInputProcessOperator<KEY, IN1, IN2, OUT>
        extends TwoInputProcessOperator<IN1, IN2, OUT> {

    @Nullable private final KeySelector<OUT, KEY> outKeySelector;

    public KeyedTwoInputProcessOperator(TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        this(userFunction, null);
    }

    public KeyedTwoInputProcessOperator(
            TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction,
            KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction);
        this.outKeySelector = outKeySelector;
    }

    @Override
    protected Consumer<OUT> getOutputCollector() {
        return outKeySelector == null ? new OutputCollector() : new KeyCheckedCollector();
    }

    @Override
    protected RuntimeContext getContext() {
        return new KeyedContextImpl();
    }

    private class KeyCheckedCollector extends OutputCollector {

        @SuppressWarnings("unchecked")
        @Override
        public void accept(OUT outputRecord) {
            try {
                KEY currentKey = (KEY) getCurrentKey();
                KEY outputKey = outKeySelector.getKey(outputRecord);
                if (!outputKey.equals(currentKey)) {
                    throw new IllegalStateException(
                            "Output key must equals to input key if you want the produced stream is keyed. ");
                }
            } catch (Exception e) {
                // TODO Change Consumer to ThrowingConsumer.
                ExceptionUtils.rethrow(e);
            }
            super.accept(outputRecord);
        }
    }

    private class OutputCollector implements Consumer<OUT> {

        private final StreamRecord<OUT> reuse = new StreamRecord<>(null);

        @Override
        public void accept(OUT outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }
    }

    // TODO Refactor runtime context for all process operators as there are some duplicate codes.
    private class KeyedContextImpl implements RuntimeContext {

        private KeyedContextImpl() {}

        @Override
        public <T> ListState<T> getState(States.ListStateDeclaration<T> stateDeclaration)
                throws Exception {
            if (!usedStates.contains(stateDeclaration)) {
                throw new IllegalArgumentException("This state is not registered.");
            }

            ListStateDescriptor<T> listStateDescriptor =
                    StateDeclarationConverter.getListStateDescriptor(stateDeclaration);
            return getOperatorStateBackend().getListState(listStateDescriptor);
        }

        @Override
        public <T> ValueState<T> getState(States.ValueStateDeclaration<T> stateDeclaration)
                throws Exception {
            if (!usedStates.contains(stateDeclaration)) {
                throw new IllegalArgumentException("This state is not registered.");
            }

            ValueStateDescriptor<T> valueStateDescriptor =
                    StateDeclarationConverter.getValueStateDescriptor(stateDeclaration);
            return getKeyedStateStore().getState(valueStateDescriptor);
        }
    }
}
