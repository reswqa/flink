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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;

public class TriggerableTwoInputKeyedProcessOperator<KEY, IN1, IN2, OUT>
        extends KeyedTwoInputProcessOperator<KEY, IN1, IN2, OUT>
        implements Triggerable<KEY, VoidNamespace> {

    private transient InternalTimerService<VoidNamespace> timerService;

    public TriggerableTwoInputKeyedProcessOperator(
            TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction, boolean sortInputs) {
        super(userFunction, sortInputs);
    }

    public TriggerableTwoInputKeyedProcessOperator(
            TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction,
            boolean sortInputs,
            KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction, sortInputs, outKeySelector);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.timerService =
                getInternalTimerService(
                        "two input process timer", VoidNamespaceSerializer.INSTANCE, this);
    }

    @Override
    protected void registerProcessingTimer(long timestamp) {
        timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // do nothing atm.
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        userFunction.onProcessingTimer(timer.getTimestamp(), getOutputCollector(), context);
    }
}
