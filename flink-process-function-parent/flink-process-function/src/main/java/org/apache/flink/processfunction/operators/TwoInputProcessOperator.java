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
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Set;
import java.util.function.Consumer;

/** Operator for {@link TwoInputStreamProcessFunction}. */
public class TwoInputProcessOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, TwoInputStreamProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private transient Consumer<OUT> collector;

    private transient RuntimeContext context;

    protected transient Set<States.StateDeclaration> usedStates;

    public TwoInputProcessOperator(TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = getOutputCollector();
        this.context = getContext();
        this.usedStates = userFunction.usesStates();
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        userFunction.processFirstInputRecord(element.getValue(), collector, context);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        userFunction.processSecondInputRecord(element.getValue(), collector, context);
    }

    protected Consumer<OUT> getOutputCollector() {
        return new OutputCollector();
    }

    protected RuntimeContext getContext() {
        return new ContextImpl();
    }

    private class OutputCollector implements Consumer<OUT> {

        private final StreamRecord<OUT> reuse = new StreamRecord<>(null);

        @Override
        public void accept(OUT outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }
    }

    // TODO Refactor runtime context for all process operators as there are some duplicate codes.
    private class ContextImpl implements RuntimeContext {

        private ContextImpl() {}

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
            throw new UnsupportedOperationException(
                    "Only keyed operator supports access keyed state.");
        }
    }
}
