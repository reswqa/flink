package org.apache.flink.streaming.runtime.watermark;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is used to handle event time related watermarks in operator, such as {@link
 * EventTimeExtension#EVENT_TIME_WATERMARK_DECLARATION} and {@link
 * EventTimeExtension#IDLE_STATUS_WATERMARK_DECLARATION}. It will emit event time watermark and idle
 * status to downstream operators according to received watermarks.
 */
public class EventTimeWatermarkHandler {

    /** number of input of operator, it should between 1 and 2 in current design. */
    private int numOfInput;

    private Output output;

    private List<EventTimeWithIdleStatus> eventTimePerInput;

    /**
     * time service manager is used to advance event time in operator, and it may be null if the
     * operator is not keyed.
     */
    @Nullable private InternalTimeServiceManager<?> timeServiceManager;

    private long lastEmitWatermark = Long.MIN_VALUE;

    private boolean lastEmitIdleStatus = false;

    public EventTimeWatermarkHandler(
            int numOfInput,
            Output output,
            @Nullable InternalTimeServiceManager<?> timeServiceManager) {
        checkArgument(numOfInput >= 1 && numOfInput <= 2, "numOfInput should between 1 and 2");
        this.numOfInput = numOfInput;
        this.output = output;
        this.eventTimePerInput = new ArrayList<>(numOfInput);
        for (int i = 0; i < numOfInput; i++) {
            eventTimePerInput.add(new EventTimeWithIdleStatus());
        }
        this.timeServiceManager = timeServiceManager;
    }

    public void processEventTime(long timestamp, int inputIndex) throws Exception {
        checkState(inputIndex < numOfInput);
        eventTimePerInput.get(inputIndex).setEventTime(timestamp);
        eventTimePerInput.get(inputIndex).setIdleStatus(false);

        tryEmitEventTimeWatermark();
    }

    private void tryEmitEventTimeWatermark() throws Exception {
        // if current event time is larger than last emit watermark, emit it
        long currentEventTime = getCurrentEventTime();
        if (currentEventTime > lastEmitWatermark) {
            output.emitWatermark(
                    new WatermarkEvent(
                            EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                                    currentEventTime),
                            false));
            lastEmitWatermark = currentEventTime;
            if (timeServiceManager != null) {
                timeServiceManager.advanceWatermark(new Watermark(currentEventTime));
            }
        }
    }

    public void processEventTimeIdleStatus(boolean isIdle, int inputIndex) {
        checkState(inputIndex < numOfInput);
        eventTimePerInput.get(inputIndex).setIdleStatus(isIdle);
        tryEmitEventTimeIdleStatus();
    }

    private void tryEmitEventTimeIdleStatus() {
        // emit idle status if current idle status is different from last emit
        boolean inputIdle = isAllInputIdle();
        if (inputIdle != lastEmitIdleStatus) {
            output.emitWatermark(
                    new WatermarkEvent(
                            EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(
                                    inputIdle),
                            false));
            lastEmitIdleStatus = inputIdle;
        }
    }

    public long getCurrentEventTime() {
        long currentEventTime = Long.MIN_VALUE;
        for (EventTimeWithIdleStatus eventTimeWithIdleStatus : eventTimePerInput) {
            if (!eventTimeWithIdleStatus.isIdle()) {
                currentEventTime =
                        Math.max(currentEventTime, eventTimeWithIdleStatus.getEventTime());
            }
        }
        return currentEventTime;
    }

    public boolean isAllInputIdle() {
        boolean allInputIsIdle = true;
        for (EventTimeWithIdleStatus eventTimeWithIdleStatus : eventTimePerInput) {
            allInputIsIdle &= eventTimeWithIdleStatus.isIdle();
        }
        return allInputIsIdle;
    }

    static class EventTimeWithIdleStatus {
        private long eventTime = Long.MIN_VALUE;
        private boolean isIdle = false;

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = Math.max(this.eventTime, eventTime);
        }

        public boolean isIdle() {
            return isIdle;
        }

        public void setIdleStatus(boolean idle) {
            isIdle = idle;
        }
    }
}
