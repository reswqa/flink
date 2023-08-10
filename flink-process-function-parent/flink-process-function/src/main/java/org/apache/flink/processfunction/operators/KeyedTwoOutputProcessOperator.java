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
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KeyedTwoOutputProcessOperator<KEY, IN, OUT_MAIN, OUT_SIDE>
        extends TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        implements Triggerable<KEY, VoidNamespace> {

    @Nullable private final KeySelector<OUT_MAIN, KEY> mainOutKeySelector;

    @Nullable private final KeySelector<OUT_SIDE, KEY> sideOutKeySelector;

    @Nullable private transient InputKeyListener inputKeyListener;

    private transient InternalTimerService<VoidNamespace> timerService;

    private final boolean sortInput;

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag,
            boolean sortInput,
            @Nullable KeySelector<OUT_MAIN, KEY> mainOutKeySelector,
            @Nullable KeySelector<OUT_SIDE, KEY> sideOutKeySelector) {
        super(userFunction, outputTag);
        this.mainOutKeySelector = mainOutKeySelector;
        this.sideOutKeySelector = sideOutKeySelector;
        this.sortInput = sortInput;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH) {
            inputKeyListener =
                    sortInput
                            ? new InputKeyListener.SortedTwoOutputInputKeyListener<>(
                                    userFunction::endOfPartition,
                                    mainCollector,
                                    sideCollector,
                                    context)
                            : new InputKeyListener.UnsortedTwoOutputInputKeyListener<>(
                                    userFunction::endOfPartition,
                                    mainCollector,
                                    sideCollector,
                                    context);
        }

        this.timerService =
                getInternalTimerService(
                        "two input process timer", VoidNamespaceSerializer.INSTANCE, this);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        setKeyContextElement(record, getStateKeySelector1());
    }

    private <T> void setKeyContextElement(StreamRecord<T> record, KeySelector<T, ?> selector)
            throws Exception {
        checkNotNull(selector);
        Object key = selector.getKey(record.getValue());
        setCurrentKey(key);
        if (inputKeyListener != null) {
            inputKeyListener.keySelected(key);
        }
    }

    @Override
    public void endInput() {
        if (!(context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH)) {
            return;
        }

        Preconditions.checkNotNull(
                inputKeyListener, "inputKeyListener must be not-null in batch mode.");
        inputKeyListener.endOfInput();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        mainCollector.setTimestamp(element);
        sideCollector.setTimestamp(element);
        userFunction.processRecord(element.getValue(), mainCollector, sideCollector, context);
    }

    @Override
    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(new MainOutputCollector(), mainOutKeySelector)
                : new MainOutputCollector();
    }

    @Override
    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(new SideOutputCollector(), sideOutKeySelector)
                : new SideOutputCollector();
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
        userFunction.onProcessingTimer(
                timer.getTimestamp(), getMainCollector(), getSideCollector(), context);
    }

    private class KeyCheckedOutputCollector<T> extends TimestampCollector<T> {

        private final TimestampCollector<T> outputCollector;

        private final KeySelector<T, KEY> outputKeySelector;

        private KeyCheckedOutputCollector(
                TimestampCollector<T> outputCollector, KeySelector<T, KEY> outputKeySelector) {
            this.outputCollector = outputCollector;
            this.outputKeySelector = outputKeySelector;
        }

        @Override
        public void collect(T outputRecord) {
            checkOutputKey(outputRecord);
            this.outputCollector.collect(outputRecord);
        }

        @Override
        public void collect(T outputRecord, long timestamp) {
            checkOutputKey(outputRecord);
            this.outputCollector.collect(outputRecord, timestamp);
        }

        @SuppressWarnings("unchecked")
        private void checkOutputKey(T outputRecord) {
            try {
                KEY currentKey = (KEY) getCurrentKey();
                KEY outputKey = this.outputKeySelector.getKey(outputRecord);
                if (!outputKey.equals(currentKey)) {
                    throw new IllegalStateException(
                            "Output key must equals to input key if you want the produced stream "
                                    + "is keyed. ");
                }
            } catch (Exception e) {
                // TODO Change Consumer to ThrowingConsumer.
                ExceptionUtils.rethrow(e);
            }
        }
    }
}
