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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KeyedTwoOutputProcessOperator<KEY, IN, OUT_MAIN, OUT_SIDE>
        extends TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE> {

    @Nullable private final KeySelector<OUT_MAIN, KEY> mainOutKeySelector;

    @Nullable private final KeySelector<OUT_SIDE, KEY> sideOutKeySelector;

    @Nullable private transient InputKeyListener inputKeyListener;

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
        userFunction.processRecord(element.getValue(), mainCollector, sideCollector, context);
    }

    @Override
    protected Consumer<OUT_MAIN> getMainCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(new MainOutputCollector(), mainOutKeySelector)
                : new MainOutputCollector();
    }

    @Override
    public Consumer<OUT_SIDE> getSideCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(new SideOutputCollector(), sideOutKeySelector)
                : new SideOutputCollector();
    }

    private class KeyCheckedOutputCollector<T> implements Consumer<T> {

        private final Consumer<T> outputCollector;

        private final KeySelector<T, KEY> outputKeySelector;

        private KeyCheckedOutputCollector(
                Consumer<T> outputCollector, KeySelector<T, KEY> outputKeySelector) {
            this.outputCollector = outputCollector;
            this.outputKeySelector = outputKeySelector;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void accept(T outputRecord) {
            try {
                KEY currentKey = (KEY) getCurrentKey();
                KEY outputKey = this.outputKeySelector.getKey(outputRecord);
                if (!outputKey.equals(currentKey)) {
                    throw new IllegalStateException(
                            "Output key must equals to input key if you want the produced stream is keyed. ");
                }
            } catch (Exception e) {
                // TODO Change Consumer to ThrowingConsumer.
                ExceptionUtils.rethrow(e);
            }
            this.outputCollector.accept(outputRecord);
        }
    }
}
