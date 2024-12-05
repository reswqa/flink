package org.apache.flink.datastream.impl.watermark;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;

import java.util.Arrays;
import java.util.Collection;

/** A specialized process function designed for extracting event timestamps. */
public class ExtractEventTimeProcessFunction<IN>
        implements OneInputStreamProcessFunction<IN, IN>,
                ProcessingTimeService.ProcessingTimeCallback {

    private final EventTimeExtractor<IN> eventTimeExtractor;

    private final EventTimeWatermarkStrategy<IN> eventTimeWatermarkStrategy;

    public ExtractEventTimeProcessFunction(
            EventTimeExtractor<IN> eventTimeExtractor,
            EventTimeWatermarkStrategy<IN> eventTimeWatermarkStrategy) {
        this.eventTimeExtractor = eventTimeExtractor;
        this.eventTimeWatermarkStrategy = eventTimeWatermarkStrategy;
    }

    @Override
    public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        return Arrays.asList(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION,
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION);
    }

    public EventTimeExtractor<IN> getEventTimeExtractor() {
        return eventTimeExtractor;
    }

    public EventTimeWatermarkStrategy<IN> getEventTimeWatermarkStrategy() {
        return eventTimeWatermarkStrategy;
    }

    @Override
    public void onProcessingTime(long time) throws Exception {
        // This method should not be called
    }

    @Override
    public void processRecord(IN record, Collector<IN> output, PartitionedContext ctx)
            throws Exception {
        // This method should not be called
    }
}
