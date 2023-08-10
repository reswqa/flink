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
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * Operator for {@link TwoOutputStreamProcessFunction}.
 *
 * <p>We support the second output by side-output.
 */
public class TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        extends AbstractUdfStreamOperator<
                OUT_MAIN, TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE>>
        implements OneInputStreamOperator<IN, OUT_MAIN>, BoundedOneInput {

    protected transient TimestampCollector<OUT_MAIN> mainCollector;

    protected transient TimestampCollector<OUT_SIDE> sideCollector;

    protected transient DefaultRuntimeContext context;

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
        this.mainCollector = getMainCollector();
        this.sideCollector = getSideCollector();
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
    public void processElement(StreamRecord<IN> element) throws Exception {
        mainCollector.setTimestamp(element);
        sideCollector.setTimestamp(element);
        userFunction.processRecord(element.getValue(), mainCollector, sideCollector, context);
    }

    @Override
    public void processWatermark(GeneralizedWatermark mark) throws Exception {
        if (timeServiceManager != null && mark instanceof TimestampWatermark) {
            timeServiceManager.advanceWatermark(
                    new Watermark(((TimestampWatermark) mark).getTimestamp()));
            output.emitWatermark(mark);
            return;
        }
        // weather emit process watermark should leave for user function.
        if (mark instanceof ProcessWatermarkWrapper) {
            userFunction.onWatermark(
                    ((ProcessWatermarkWrapper) mark).getProcessWatermark(),
                    getMainCollector(),
                    getSideCollector(),
                    context);
        }
    }

    @Override
    public void endInput() throws Exception {
        if (!(context.getExecutionMode() == RuntimeContext.ExecutionMode.BATCH)) {
            return;
        }
        userFunction.endOfPartition(mainCollector, sideCollector, context);
    }

    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return new MainOutputCollector();
    }

    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return new SideOutputCollector();
    }

    protected void registerProcessingTimer(long timeStamp) {
        throw new UnsupportedOperationException(
                "Only triggerable keyed operator supports register processing timer.");
    }

    protected abstract static class TimestampCollector<T> implements Collector<T> {
        protected final StreamRecord<T> reuse = new StreamRecord<>(null);

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
    }

    protected class MainOutputCollector extends TimestampCollector<OUT_MAIN> {
        @Override
        public void collect(OUT_MAIN outputRecord) {
            output.collect(reuse.replace(outputRecord));
        }

        @Override
        public void collect(OUT_MAIN record, long timestamp) {
            setAbsoluteTimestamp(timestamp);
            output.collect(reuse.replace(record));
        }
    }

    protected class SideOutputCollector extends TimestampCollector<OUT_SIDE> {

        @Override
        public void collect(OUT_SIDE outputRecord) {
            output.collect(outputTag, reuse.replace(outputRecord));
        }

        @Override
        public void collect(OUT_SIDE record, long timestamp) {
            setAbsoluteTimestamp(timestamp);
            output.collect(outputTag, reuse.replace(record));
        }
    }
}
