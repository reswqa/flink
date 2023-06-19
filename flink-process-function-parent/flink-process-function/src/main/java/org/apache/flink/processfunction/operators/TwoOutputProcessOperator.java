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
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Operator for {@link TwoOutputStreamProcessFunction}.
 *
 * <p>We support the second output by side-output.
 */
public class TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        extends AbstractUdfStreamOperator<
                OUT_MAIN, TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE>>
        implements OneInputStreamOperator<IN, OUT_MAIN> {

    protected transient Consumer<OUT_MAIN> mainCollector;

    protected transient Consumer<OUT_SIDE> sideCollector;

    protected transient RuntimeContext context;

    protected transient Set<States.StateDeclaration> usedStates;

    protected OutputTag<OUT_SIDE> outputTag;

    public TwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag) {
        super(userFunction);

        this.outputTag = outputTag;
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        this.usedStates = userFunction.usesStates();
        this.mainCollector = new MainOutputCollector();
        this.sideCollector = new SideOutputCollector();
        this.context = new ContextImpl();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        userFunction.processRecord(element.getValue(), mainCollector, sideCollector, context);
    }

    protected RuntimeContext getContext() {
        return new ContextImpl();
    }

    protected Consumer<OUT_MAIN> getMainOutputCollector() {
        return new MainOutputCollector();
    }

    private class MainOutputCollector implements Consumer<OUT_MAIN> {

        private final StreamRecord<OUT_MAIN> reuse = new StreamRecord<>(null);

        @Override
        public void accept(OUT_MAIN outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }
    }

    private class SideOutputCollector implements Consumer<OUT_SIDE> {
        private final StreamRecord<OUT_SIDE> reuse = new StreamRecord<>(null);

        @Override
        public void accept(OUT_SIDE outputRecord) {
            output.collect(outputTag, reuse.replace(outputRecord));
        }
    }

    private class ContextImpl implements RuntimeContext {

        private ContextImpl() {}

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
            throw new UnsupportedOperationException(
                    "Only keyed operator supports access keyed state.");
        }
    }
}
