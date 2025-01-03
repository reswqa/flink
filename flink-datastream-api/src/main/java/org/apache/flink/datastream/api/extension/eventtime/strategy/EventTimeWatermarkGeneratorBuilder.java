package org.apache.flink.datastream.api.extension.eventtime.strategy;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;

import java.time.Duration;

/**
 * A utility class for constructing a processing function that extracts event time and generates
 * event time watermarks.
 */
public class EventTimeWatermarkGeneratorBuilder<T> {

    private final EventTimeWatermarkStrategy<T> strategy;

    public EventTimeWatermarkGeneratorBuilder(EventTimeExtractor<T> eventTimeExtractor) {
        this.strategy = new EventTimeWatermarkStrategy<>(eventTimeExtractor);
    }

    public EventTimeWatermarkGeneratorBuilder<T> noWatermark() {
        this.strategy.setGenerateMode(
                EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.NO_WATERMARK);
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> periodicWatermark(
            Duration periodicWatermarkInterval) {
        this.strategy.setGenerateMode(
                EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC);
        this.strategy.setPeriodicWatermarkInterval(periodicWatermarkInterval);
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> perEventWatermark() {
        this.strategy.setGenerateMode(
                EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT);
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> withIdleness(Duration idleTimeout) {
        this.strategy.setEnableIdleStatus(true);
        this.strategy.setIdleTimeout(idleTimeout);
        return this;
    }

    public EventTimeWatermarkGeneratorBuilder<T> withMaxOutOfOrderTime(Duration maxOutOfOrderTime) {
        this.strategy.setMaxOutOfOrderTime(maxOutOfOrderTime);
        return this;
    }

    public OneInputStreamProcessFunction<T, T> buildAsProcessFunction() {
        return EventTimeExtension.generateProcessFunctionWithWatermarkStrategy(this.strategy);
    }
}
