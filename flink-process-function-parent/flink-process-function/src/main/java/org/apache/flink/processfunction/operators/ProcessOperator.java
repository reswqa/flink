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
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Operator for {@link SingleStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, SingleStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    protected transient DefaultRuntimeContext context;

    protected transient OutputCollector outputCollector;

    public ProcessOperator(SingleStreamProcessFunction<IN, OUT> userFunction) {
        super(userFunction);

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        context =
                new DefaultRuntimeContext(
                        userFunction.usesStates(),
                        getOperatorStateBackend(),
                        getRuntimeContext(),
                        this::getCurrentKey,
                        this::registerProcessingTimer,
                        output);
        outputCollector = getOutputCollector();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        outputCollector.setTimestamp(element);
        userFunction.processRecord(element.getValue(), outputCollector, context);
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
            return;
        }
        // weather emit process watermark should leave for user function.
        if (mark instanceof ProcessWatermarkWrapper) {
            userFunction.onWatermark(
                    ((ProcessWatermarkWrapper) mark).getProcessWatermark(),
                    outputCollector,
                    context);
        }
    }

    protected OutputCollector getOutputCollector() {
        return new OutputCollector();
    }

    protected void registerProcessingTimer(long timeStamp) {
        throw new UnsupportedOperationException(
                "Only triggerable keyed operator supports register processing timer.");
    }

    @Override
    public void endInput() throws Exception {
        if (!(context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH)) {
            return;
        }
        userFunction.endOfPartition(outputCollector, context);
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
