package org.apache.flink.streaming.util.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;

import java.util.function.Consumer;

/**
 * A class used to combine {@link Watermark}s. The combiner will be created by {@link
 * WatermarkDeclaration} according to the {@link WatermarkCombinationPolicy}.
 */
@Internal
public interface WatermarkCombiner {

    void combineWatermark(
            Watermark watermark, int channelIndex, Consumer<Watermark> watermarkEmitter)
            throws Exception;
}
