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
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.function.Consumer;

/** Operator for {@link SingleStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, SingleStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    protected transient RuntimeContext context;

    protected transient Consumer<OUT> outputCollector;

    public ProcessOperator(SingleStreamProcessFunction<IN, OUT> userFunction) {
        super(userFunction);

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        context =
                new DefaultRuntimeContext(
                        userFunction.usesStates(), getOperatorStateBackend(), getKeyedStateStore());
        outputCollector = getOutputCollector();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        userFunction.processRecord(element.getValue(), outputCollector, context);
    }

    protected Consumer<OUT> getOutputCollector() {
        return new OutputCollector();
    }

    private class OutputCollector implements Consumer<OUT> {

        private final StreamRecord<OUT> reuse = new StreamRecord<>(null);

        @Override
        public void accept(OUT outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }
    }
}
