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

import org.apache.flink.processfunction.DefaultRuntimeContext;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.function.Consumer;

/** Operator for {@link TwoInputStreamProcessFunction}. */
public class TwoInputProcessOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, TwoInputStreamProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {

    protected transient Consumer<OUT> collector;

    protected transient DefaultRuntimeContext context;

    public TwoInputProcessOperator(TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = getOutputCollector();
        this.context =
                new DefaultRuntimeContext(
                        userFunction.usesStates(),
                        getOperatorStateBackend(),
                        getRuntimeContext(),
                        this::getCurrentKey);
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

    @Override
    public void endInput(int inputId) throws Exception {
        if (!(context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH)) {
            return;
        }
        // sanity check.
        Preconditions.checkState(inputId >= 1 && inputId <= 2);
        if (inputId == 1) {
            userFunction.endOfFirstInputPartition(collector, context);
        } else {
            userFunction.endOfSecondInputPartition(collector, context);
        }
    }

    private class OutputCollector implements Consumer<OUT> {

        private final StreamRecord<OUT> reuse = new StreamRecord<>(null);

        @Override
        public void accept(OUT outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }
    }
}
