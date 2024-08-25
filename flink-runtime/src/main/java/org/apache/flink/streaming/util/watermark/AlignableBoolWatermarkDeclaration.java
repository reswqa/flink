package org.apache.flink.streaming.util.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;

/** An alignable {@link BoolWatermarkDeclaration}. */
@Internal
public class AlignableBoolWatermarkDeclaration extends BoolWatermarkDeclaration
        implements Alignable {
    private final boolean isAligned;

    public AlignableBoolWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicyForChannel,
            WatermarkHandlingStrategy defaultHandlingStrategyForFunction,
            boolean isAligned) {
        super(identifier, combinationPolicyForChannel, defaultHandlingStrategyForFunction);
        this.isAligned = isAligned;
    }

    public boolean isAligned() {
        return isAligned;
    }
}
