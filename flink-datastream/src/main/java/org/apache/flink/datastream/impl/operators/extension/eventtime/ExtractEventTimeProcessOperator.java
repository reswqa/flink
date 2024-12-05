package org.apache.flink.datastream.impl.operators.extension.eventtime;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkGenerator;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkOutput;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;
import org.apache.flink.datastream.impl.operators.ProcessOperator;
import org.apache.flink.datastream.impl.watermark.ExtractEventTimeProcessFunction;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.clock.RelativeClock;

/**
 * Operator for {@link ExtractEventTimeProcessFunction}. This operator extracts the event time from
 * the input stream and send event time watermark to the output stream.
 */
public class ExtractEventTimeProcessOperator<IN> extends ProcessOperator<IN, IN>
        implements ProcessingTimeService.ProcessingTimeCallback {

    protected final EventTimeExtractor<IN> eventTimeExtractor;

    protected EventTimeWatermarkGenerator<IN> eventTimeWatermarkGenerator;

    protected EventTimeWatermarkOutputImpl<IN> eventTimeWatermarkOutput;

    // the interval of periodic event time watermark
    private long watermarkInterval;

    public ExtractEventTimeProcessOperator(ExtractEventTimeProcessFunction<IN> userFunction) {
        super(userFunction);

        this.eventTimeExtractor = userFunction.getEventTimeExtractor();
    }

    @Override
    public void open() throws Exception {
        super.open();
        eventTimeWatermarkOutput = new EventTimeWatermarkOutputImpl<>(nonPartitionedContext);

        watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        if (watermarkInterval > 0) {
            final long now = getProcessingTimeService().getCurrentProcessingTime();
            getProcessingTimeService().registerTimer(now + watermarkInterval, this);
        }

        this.eventTimeWatermarkGenerator =
                ((ExtractEventTimeProcessFunction<IN>) userFunction)
                        .getEventTimeWatermarkStrategy()
                        .createEventTimeWatermarkGenerator(
                                new EventTimeWatermarkStrategy
                                        .EventTimeWatermarkGeneratorContext() {

                                    private PausableRelativeClock inputActivityClock;

                                    @Override
                                    public MetricGroup getMetricGroup() {
                                        return ExtractEventTimeProcessOperator.this
                                                .getMetricGroup();
                                    }

                                    @Override
                                    public RelativeClock getInputActivityClock() {
                                        if (inputActivityClock == null) {
                                            inputActivityClock =
                                                    new PausableRelativeClock(
                                                            getProcessingTimeService().getClock());
                                        }
                                        return inputActivityClock;
                                    }
                                });
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        long extractedEventTime = eventTimeExtractor.extractTimestamp(element.getValue());
        eventTimeWatermarkGenerator.onEvent(
                element.getValue(), extractedEventTime, eventTimeWatermarkOutput);
        outputCollector.collectAndOverwriteTimestamp(element.getValue(), extractedEventTime);
    }

    @Override
    public void onProcessingTime(long time) throws Exception {
        eventTimeWatermarkGenerator.onPeriodicEmit(eventTimeWatermarkOutput);
        final long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }

    public static class EventTimeWatermarkOutputImpl<IN> implements EventTimeWatermarkOutput {

        private NonPartitionedContext<IN> nonPartitionedContext;

        public EventTimeWatermarkOutputImpl(NonPartitionedContext<IN> nonPartitionedContext) {
            this.nonPartitionedContext = nonPartitionedContext;
        }

        @Override
        public void emitEventTimeWatermark(long eventTimeWatermark) {
            nonPartitionedContext
                    .getWatermarkManager()
                    .emitWatermark(
                            EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                                    eventTimeWatermark));
        }

        @Override
        public void emitEventTimeWatermarkIdle(boolean isIdle) {
            nonPartitionedContext
                    .getWatermarkManager()
                    .emitWatermark(
                            EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(
                                    isIdle));
        }
    }
}
