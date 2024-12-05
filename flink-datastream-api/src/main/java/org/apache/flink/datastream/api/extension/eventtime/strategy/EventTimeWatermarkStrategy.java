package org.apache.flink.datastream.api.extension.eventtime.strategy;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;

import java.io.Serializable;
import java.time.Duration;

/**
 * This class represents how and when to extract event time and watermarks in the Event Time
 * Extension.
 */
public class EventTimeWatermarkStrategy<T> implements Serializable {
    // how to extract event time from event
    private EventTimeExtractor<T> eventTimeExtractor;

    // what frequency to generate event time watermark
    private EventTimeWatermarkGenerateMode generateMode =
            EventTimeWatermarkGenerateMode.NO_WATERMARK;
    private Duration periodicWatermarkInterval = Duration.ZERO;

    // whether enable idle status
    private boolean enableIdleStatus = false;
    private Duration idleTimeout = Duration.ZERO;

    // max out-of-order time
    private Duration maxOutOfOrderTime = Duration.ZERO;

    public EventTimeWatermarkStrategy(EventTimeExtractor<T> eventTimeExtractor) {
        this.eventTimeExtractor = eventTimeExtractor;
    }

    public OneInputStreamProcessFunction<T, T> buildAsProcessFunction() {
        return EventTimeExtension.generateProcessFunctionWithWatermarkStrategy(this);
    }

    public EventTimeExtractor<T> getEventTimeExtractor() {
        return eventTimeExtractor;
    }

    public void setEventTimeExtractor(EventTimeExtractor<T> eventTimeExtractor) {
        this.eventTimeExtractor = eventTimeExtractor;
    }

    public EventTimeWatermarkGenerateMode getGenerateMode() {
        return generateMode;
    }

    public void setGenerateMode(EventTimeWatermarkGenerateMode generateMode) {
        this.generateMode = generateMode;
    }

    public Duration getPeriodicWatermarkInterval() {
        return periodicWatermarkInterval;
    }

    public void setPeriodicWatermarkInterval(Duration periodicWatermarkInterval) {
        this.periodicWatermarkInterval = periodicWatermarkInterval;
    }

    public boolean isEnableIdleStatus() {
        return enableIdleStatus;
    }

    public void setEnableIdleStatus(boolean enableIdleStatus) {
        this.enableIdleStatus = enableIdleStatus;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Duration getMaxOutOfOrderTime() {
        return maxOutOfOrderTime;
    }

    public void setMaxOutOfOrderTime(Duration maxOutOfOrderTime) {
        this.maxOutOfOrderTime = maxOutOfOrderTime;
    }

    public enum EventTimeWatermarkGenerateMode {
        NO_WATERMARK,
        PERIODIC,
        PER_EVENT;
    }
}
