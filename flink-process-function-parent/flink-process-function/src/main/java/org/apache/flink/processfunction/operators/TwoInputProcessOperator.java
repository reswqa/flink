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

import org.apache.flink.api.common.eventtime.GeneralizedWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermarkWrapper;
import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.processfunction.DefaultRuntimeContext;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/** Operator for {@link TwoInputStreamProcessFunction}. */
public class TwoInputProcessOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, TwoInputStreamProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {

    protected transient OutputCollector collector;

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
                        this::getCurrentKey,
                        this::registerProcessingTimer,
                        output);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.processFirstInputRecord(element.getValue(), collector, context);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.processSecondInputRecord(element.getValue(), collector, context);
    }

    @Override
    public void processWatermark(GeneralizedWatermark mark) throws Exception {
        if (mark instanceof TimestampWatermark) {
            if (timeServiceManager != null) {
                timeServiceManager.advanceWatermark(
                        new Watermark(((TimestampWatermark) mark).getTimestamp()));
            }
            // TODO should we trigger userFunction.onWatermark also for timestamp watermark?
            // always push timestamp watermark to output be keep the original behavior.
            output.emitWatermark(mark);
        }
    }

    @Override
    protected boolean processWatermark(GeneralizedWatermark mark, int index) throws Exception {
        boolean isAligned = super.processWatermark(mark, index);
        if (mark instanceof ProcessWatermarkWrapper) {
            if (isAligned) {
                userFunction.onWatermark(
                        ((ProcessWatermarkWrapper) mark).getProcessWatermark(),
                        context,
                        getOutputCollector(),
                        TwoInputStreamProcessFunction.WatermarkType.ALL);
            } else {
                userFunction.onWatermark(
                        ((ProcessWatermarkWrapper) mark).getProcessWatermark(),
                        context,
                        getOutputCollector(),
                        index == 0
                                ? TwoInputStreamProcessFunction.WatermarkType.FIRST
                                : TwoInputStreamProcessFunction.WatermarkType.SECOND);
            }
        }
        return isAligned;
    }

    protected OutputCollector getOutputCollector() {
        return new OutputCollector();
    }

    protected void registerProcessingTimer(long timestamp) {
        throw new UnsupportedOperationException(
                "Only triggerable keyed operator supports register processing timer.");
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

    protected class OutputCollector implements Collector<OUT> {
        protected final StreamRecord<OUT> reuse = new StreamRecord<>(null);

        public void setTimestamp(StreamRecord<?> timestampBase) {
            if (timestampBase.hasTimestamp()) {
                setAbsoluteTimestamp(timestampBase.getTimestamp());
            } else {
                eraseTimestamp();
            }
        }

        public void setAbsoluteTimestamp(long timestamp) {
            reuse.setTimestamp(timestamp);
        }

        public void eraseTimestamp() {
            reuse.eraseTimestamp();
        }

        @Override
        public void collect(OUT outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }

        @Override
        public void collect(OUT record, long timestamp) {
            setAbsoluteTimestamp(timestamp);
            output.collect(reuse.replace(record));
        }
    }
}
