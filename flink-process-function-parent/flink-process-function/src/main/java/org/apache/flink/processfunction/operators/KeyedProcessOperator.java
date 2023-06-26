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
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operator for {@link SingleStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class KeyedProcessOperator<KEY, IN, OUT> extends ProcessOperator<IN, OUT> {

    @Nullable private final KeySelector<OUT, KEY> outKeySelector;

    @Nullable private InputKeyListener<OUT> inputKeyListener;

    private final boolean sortInputs;

    public KeyedProcessOperator(
            SingleStreamProcessFunction<IN, OUT> userFunction, boolean sortInputs) {
        this(userFunction, sortInputs, null);
    }

    public KeyedProcessOperator(
            SingleStreamProcessFunction<IN, OUT> userFunction,
            boolean sortInputs,
            KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction);
        this.sortInputs = sortInputs;
        this.outKeySelector = outKeySelector;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH) {
            inputKeyListener =
                    sortInputs
                            ? new InputKeyListener.SortedInputKeyListener<>(
                                    outputCollector, context, userFunction::endOfPartition)
                            : new InputKeyListener.UnSortedInputKeyListener<>(
                                    outputCollector, context, userFunction::endOfPartition);
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
        userFunction.processRecord(element.getValue(), outputCollector, context);
    }

    @Override
    protected Consumer<OUT> getOutputCollector() {
        return outKeySelector != null ? new KeyCheckedCollector() : new OutputCollector();
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
}
