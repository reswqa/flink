package org.apache.flink.datastream.impl.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkManager;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.util.clock.Clock;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkState;

/** A specialized process function designed for extracting event timestamps. */
public class ExtractEventTimeProcessFunction<IN>
        implements OneInputStreamProcessFunction<IN, IN>,
                ProcessingTimeService.ProcessingTimeCallback {

    private final EventTimeWatermarkStrategy<IN> watermarkStrategy;

    /** The maximum timestamp encountered so far. */
    private long currentMaxEventTime;

    private long watermarkInterval = 0;

    private IdlenessTimer idlenessTimer;

    private boolean isIdleNow = false;

    private ProcessingTimeService processingTimeService;

    private WatermarkManager watermarkManager;

    public ExtractEventTimeProcessFunction(EventTimeWatermarkStrategy<IN> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    public void init(
            ExecutionConfig config,
            PartitionedContext ctx,
            ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
        this.watermarkManager = ctx.getNonPartitionedContext().getWatermarkManager();

        if (watermarkStrategy.getGenerateMode()
                == EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC) {
            if (watermarkStrategy.getPeriodicWatermarkInterval().isZero()) {
                watermarkInterval = config.getAutoWatermarkInterval();
            } else {
                watermarkInterval = watermarkStrategy.getPeriodicWatermarkInterval().toMillis();
            }

            checkState(watermarkInterval > 0, "Invalid watermark interval: " + watermarkInterval);
            processingTimeService.registerTimer(
                    processingTimeService.getCurrentProcessingTime() + watermarkInterval, this);
        }

        if (watermarkStrategy.isEnableIdleStatus()) {
            idlenessTimer =
                    new IdlenessTimer(processingTimeService, watermarkStrategy.getIdleTimeout());
        }
    }

    @Override
    public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        ArrayList<WatermarkDeclaration> watermarkDeclarations = new ArrayList<>();
        watermarkDeclarations.add(EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION);

        if (watermarkStrategy.isEnableIdleStatus()) {
            watermarkDeclarations.add(EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION);
        }
        return watermarkDeclarations;
    }

    @Override
    public void processRecord(IN record, Collector<IN> output, PartitionedContext ctx)
            throws Exception {
        long extractedEventTime =
                watermarkStrategy.getEventTimeExtractor().extractTimestamp(record);
        currentMaxEventTime = Math.max(currentMaxEventTime, extractedEventTime);
        output.collectAndOverwriteTimestamp(record, extractedEventTime);

        if (watermarkStrategy.getGenerateMode()
                == EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT) {
            long emittedEventTimeWatermark =
                    currentMaxEventTime - watermarkStrategy.getMaxOutOfOrderTime().toMillis();
            ctx.getNonPartitionedContext()
                    .getWatermarkManager()
                    .emitWatermark(
                            EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                                    emittedEventTimeWatermark));
        }

        if (watermarkStrategy.isEnableIdleStatus()) {
            idlenessTimer.activity();
            isIdleNow = false;
        }
    }

    @Override
    public void onProcessingTimer(long timestamp, Collector<IN> output, PartitionedContext ctx) {
        if (watermarkStrategy.isEnableIdleStatus() && idlenessTimer.checkIfIdle()) {
            if (!isIdleNow) {
                ctx.getNonPartitionedContext()
                        .getWatermarkManager()
                        .emitWatermark(
                                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(
                                        true));
                isIdleNow = true;
            }
        } else {
            long emittedEventTimeWatermark =
                    currentMaxEventTime - watermarkStrategy.getMaxOutOfOrderTime().toMillis();
            ctx.getNonPartitionedContext()
                    .getWatermarkManager()
                    .emitWatermark(
                            EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                                    emittedEventTimeWatermark));
        }

        ctx.getProcessingTimeManager().registerTimer(timestamp + watermarkInterval);
    }

    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {
        if (watermarkStrategy.isEnableIdleStatus() && idlenessTimer.checkIfIdle()) {
            if (!isIdleNow) {
                watermarkManager.emitWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true));
                isIdleNow = true;
            }
        } else {
            long emittedEventTimeWatermark =
                    currentMaxEventTime - watermarkStrategy.getMaxOutOfOrderTime().toMillis();
            watermarkManager.emitWatermark(
                    EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                            emittedEventTimeWatermark));
        }

        processingTimeService.registerTimer(
                processingTimeService.getCurrentProcessingTime() + watermarkInterval, this);
    }

    // see org.apache.flink.api.common.eventtime.WatermarksWithIdleness.IdlenessTimer
    @VisibleForTesting
    public static final class IdlenessTimer {

        /** The clock used to measure elapsed time. */
        private final ProcessingTimeService timeService;

        /** Counter to detect change. No problem if it overflows. */
        private long counter;

        /** The value of the counter at the last activity check. */
        private long lastCounter;

        /**
         * The first time (relative to {@link Clock#relativeTimeNanos()}) when the activity check
         * found that no activity happened since the last check. Special value: 0 = no timer.
         */
        private long startOfInactivity;

        /** The duration before the output is marked as idle. */
        private final long maxIdleTimeout;

        public IdlenessTimer(ProcessingTimeService timeService, Duration idleTimeout) {
            this.timeService = timeService;
            this.maxIdleTimeout = idleTimeout.toMillis();
        }

        public void activity() {
            counter++;
        }

        public boolean checkIfIdle() {
            if (counter != lastCounter) {
                // activity since the last check. we reset the timer
                lastCounter = counter;
                startOfInactivity = 0L;
                return false;
            } else // timer started but has not yet reached idle timeout
            if (startOfInactivity == 0L) {
                // first time that we see no activity since the last periodic probe
                // begin the timer
                startOfInactivity = timeService.getCurrentProcessingTime();
                return false;
            } else {
                return timeService.getCurrentProcessingTime() - startOfInactivity > maxIdleTimeout;
            }
        }
    }
}
