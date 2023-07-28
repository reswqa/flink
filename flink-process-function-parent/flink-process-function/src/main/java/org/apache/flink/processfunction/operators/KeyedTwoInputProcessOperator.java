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
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/** Operator for {@link TwoInputStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class KeyedTwoInputProcessOperator<KEY, IN1, IN2, OUT>
        extends TwoInputProcessOperator<IN1, IN2, OUT> {

    @Nullable private final KeySelector<OUT, KEY> outKeySelector;

    /** Only NonNull in batch mode with keyed input. */
    @Nullable private transient InputKeyListener inputKeyListener1;

    /** Only NonNull in batch mode with keyed input. */
    @Nullable private transient InputKeyListener inputKeyListener2;

    private final boolean sortInputs;

    public KeyedTwoInputProcessOperator(
            TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction, boolean sortInputs) {
        this(userFunction, sortInputs, null);
    }

    public KeyedTwoInputProcessOperator(
            TwoInputStreamProcessFunction<IN1, IN2, OUT> userFunction,
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
            if (getStateKeySelector1() != null) {
                inputKeyListener1 =
                        sortInputs
                                ? new InputKeyListener.SortedInputKeyListener<>(
                                        collector, context, userFunction::endOfFirstInputPartition)
                                : new InputKeyListener.UnSortedInputKeyListener<>(
                                        collector, context, userFunction::endOfFirstInputPartition);
            }

            if (getStateKeySelector2() != null) {
                inputKeyListener2 =
                        sortInputs
                                ? new InputKeyListener.SortedInputKeyListener<>(
                                        collector, context, userFunction::endOfSecondInputPartition)
                                : new InputKeyListener.UnSortedInputKeyListener<>(
                                        collector,
                                        context,
                                        userFunction::endOfSecondInputPartition);
            }
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        // TODO FLINK-30601 Omit "setKeyContextElement" call for non-keyed stream/operators to
        // improve performance if operator is not keyed and does not override setKeyContextElement1
        // setKeyContextElement2. We do depend on this method to dispatch end of keyed partition, as
        // a result, may cause some performance regression for keyed-broadcast co-process. We should
        // re-consider this, to some extent, the previous design was also a bit strange.
        setKeyContextElement(record, getStateKeySelector1(), inputKeyListener1);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement2(StreamRecord record) throws Exception {
        // TODO FLINK-30601 Omit "setKeyContextElement" call for non-keyed stream/operators to
        // improve performance if operator is not keyed and does not override setKeyContextElement1
        // setKeyContextElement2. We do depend on this method to dispatch end of keyed partition, as
        // a result, may cause some performance regression for keyed-broadcast co-process. We should
        // re-consider this, to some extent, the previous design was also a bit strange.
        setKeyContextElement(record, getStateKeySelector2(), inputKeyListener2);
    }

    private <T> void setKeyContextElement(
            StreamRecord<T> record, KeySelector<T, ?> selector, InputKeyListener inputKeyListener)
            throws Exception {
        if (selector == null) {
            return;
        }
        Object key = selector.getKey(record.getValue());
        setCurrentKey(key);
        if (inputKeyListener != null) {
            inputKeyListener.keySelected(key);
        }
    }

    @Override
    protected OutputCollector getOutputCollector() {
        return outKeySelector == null ? new OutputCollector() : new KeyCheckedCollector();
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (!(context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH)) {
            return;
        }
        // sanity check.
        Preconditions.checkState(inputId >= 1 && inputId <= 2);
        if (inputId == 1) {
            if (inputKeyListener1 != null) {
                inputKeyListener1.endOfInput();
            } else {
                userFunction.endOfFirstInputPartition(collector, context);
            }
        } else {
            if (inputKeyListener2 != null) {
                inputKeyListener2.endOfInput();
            } else {
                userFunction.endOfSecondInputPartition(collector, context);
            }
        }
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
}
