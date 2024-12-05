package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultTwoOutputNonPartitionedContext;
import org.apache.flink.datastream.impl.operators.extension.eventtime.WithEventTimeExtension;
import org.apache.flink.datastream.impl.watermark.ExtractEventTimeProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link EventTimeExtension}. */
public class EventTimeExtensionImpl {

    public static <T> OneInputStreamProcessFunction<T, T> extractEventTimeAndWatermark(
            EventTimeExtractor<T> eventTimeExtractor,
            EventTimeWatermarkStrategy<T> eventTimeWatermarkStrategy) {
        return new ExtractEventTimeProcessFunction<>(
                eventTimeExtractor, eventTimeWatermarkStrategy);
    }

    public static <T> EventTimeManager getEventTimeManager(NonPartitionedContext<T> ctx)
            throws Exception {
        checkState(ctx instanceof DefaultNonPartitionedContext);
        DefaultNonPartitionedContext<T> defaultNonPartitionedContext =
                (DefaultNonPartitionedContext<T>) ctx;

        AbstractStreamOperator operator = defaultNonPartitionedContext.getOperator();

        // only keyed operator can get EventTimeManager.
        if (!(operator instanceof WithEventTimeExtension)) {
            throw new UnsupportedOperationException(
                    "Non-keyed stream can not get EventTimeManager.");
        }

        // init event time extension in operator
        ((WithEventTimeExtension) operator).initEventTimeExtension();
        return ((WithEventTimeExtension) operator).getEventTimeManager();
    }

    public <T1, T2> EventTimeManager getEventTimeManager(TwoOutputNonPartitionedContext<T1, T2> ctx)
            throws Exception {
        checkState(ctx instanceof DefaultTwoOutputNonPartitionedContext);
        DefaultTwoOutputNonPartitionedContext defaultNonPartitionedContext =
                (DefaultTwoOutputNonPartitionedContext) ctx;

        AbstractStreamOperator operator = defaultNonPartitionedContext.getOperator();

        // only keyed operator can get EventTimeManager.
        if (!(operator instanceof WithEventTimeExtension)) {
            throw new UnsupportedOperationException(
                    "Non-keyed stream can not get EventTimeManager.");
        }

        // init event time extension in operator
        ((WithEventTimeExtension) operator).initEventTimeExtension();
        return ((WithEventTimeExtension) operator).getEventTimeManager();
    }
}
