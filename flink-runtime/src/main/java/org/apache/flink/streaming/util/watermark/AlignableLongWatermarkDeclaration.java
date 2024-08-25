package org.apache.flink.streaming.util.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;

/** An alignable {@link LongWatermarkDeclaration}. */
@Internal
public class AlignableLongWatermarkDeclaration extends LongWatermarkDeclaration
        implements Alignable {

    private final boolean isAligned;

    public AlignableLongWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction,
            boolean isAligned) {
        super(identifier, combinationPolicyForChannel, defaultHandlingStrategyForFunction);
        this.isAligned = isAligned;
    }

    @Override
    public boolean isAligned() {
        return isAligned;
    }
}
