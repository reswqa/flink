package org.apache.flink.streaming.runtime.watermark.extension.eventtime;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.EventTimeWatermarkCombiner;
import org.apache.flink.streaming.runtime.watermark.EventTimeWatermarkHandler;
import org.apache.flink.streaming.runtime.watermark.WatermarkCombiner;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Util class for {@link EventTimeExtension}. */
public class EventTimeExtensionUtil {

    public static boolean processWatermark(
            Watermark watermark,
            int inputIndex,
            EventTimeWatermarkHandler eventTimeWatermarkHandler)
            throws Exception {
        if (EventTimeExtension.isEventTimeWatermarkDeclaration(watermark.getIdentifier())) {
            long timestamp = ((LongWatermark) watermark).getValue();
            eventTimeWatermarkHandler.processEventTime(timestamp, inputIndex);
            return true;
        } else if (EventTimeExtension.isIdleStatusWatermarkDeclaration(watermark.getIdentifier())) {
            boolean isIdle = ((BoolWatermark) watermark).getValue();
            eventTimeWatermarkHandler.processEventTimeIdleStatus(isIdle, inputIndex);
            return true;
        }
        return false;
    }

    /** Create watermark combiners if there are event time watermark declarations. */
    public static void addWatermarkCombinerIfNeeded(
            Set<AbstractInternalWatermarkDeclaration<?>> watermarkDeclarationSet,
            Map<String, WatermarkCombiner> watermarkCombiners,
            int numberOfInputChannels) {
        Set<String> declaredWatermarkIdentifiers =
                watermarkDeclarationSet.stream()
                        .map(AbstractInternalWatermarkDeclaration::getIdentifier)
                        .collect(Collectors.toSet());

        // create event time watermark combiner
        if (declaredWatermarkIdentifiers.contains(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.getIdentifier())) {
            EventTimeWatermarkCombiner eventTimeWatermarkCombiner =
                    new EventTimeWatermarkCombiner(numberOfInputChannels);
            watermarkCombiners.put(
                    EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.getIdentifier(),
                    eventTimeWatermarkCombiner);
            watermarkCombiners.put(
                    EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.getIdentifier(),
                    eventTimeWatermarkCombiner);
        }
    }
}
