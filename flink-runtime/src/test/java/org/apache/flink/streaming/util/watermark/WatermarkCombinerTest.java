package org.apache.flink.streaming.util.watermark;

import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WatermarkCombiner}. */
public class WatermarkCombinerTest {

    private static final String DEFAULT_WATERMARK_IDENTIFIER = "default";

    @Test
    public void testAlignedWatermarkCombiner() throws Exception {
        InternalLongWatermarkDeclaration watermarkDeclaration =
                new InternalLongWatermarkDeclaration(
                        DEFAULT_WATERMARK_IDENTIFIER,
                        new WatermarkCombinationPolicy(
                                WatermarkCombinationFunction.NumericWatermarkCombinationFunction
                                        .MIN,
                                true),
                        WatermarkHandlingStrategy.FORWARD,
                        true);
        AtomicBoolean gateNotified = new AtomicBoolean(false);
        List<Watermark> receivedWatermarks = new ArrayList<>();
        AlignedWatermarkCombiner combiner =
                new AlignedWatermarkCombiner(2, () -> gateNotified.set(true));

        // send first watermark to channel 0
        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(1L), 0, receivedWatermarks::add);
        assertThat(gateNotified.get()).isFalse();
        assertThat(receivedWatermarks).isEmpty();

        // send first watermark to channel 1
        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(1L), 1, receivedWatermarks::add);
        assertThat(gateNotified.get()).isTrue();
        assertThat(receivedWatermarks).hasSize(1);
        assertThat(receivedWatermarks.get(0)).isInstanceOf(LongWatermark.class);
        assertThat(((LongWatermark) receivedWatermarks.get(0)).getValue()).isEqualTo(1);

        // clear state
        gateNotified.set(false);
        receivedWatermarks.clear();

        // send second watermark to channel 1
        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(1L), 1, receivedWatermarks::add);
        assertThat(gateNotified.get()).isFalse();
        assertThat(receivedWatermarks).isEmpty();
    }

    @Test
    public void testLongWatermarkCombineMAX() throws Exception {
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .build();

        InternalLongWatermarkDeclaration internalWatermarkDeclaration =
                (InternalLongWatermarkDeclaration)
                        AbstractInternalWatermarkDeclaration.from(watermarkDeclaration);

        List<Watermark> receivedWatermarks = new ArrayList<>();

        WatermarkCombiner combiner = internalWatermarkDeclaration.watermarkCombiner(2, null);

        combiner.combineWatermark(
                internalWatermarkDeclaration.newWatermark(1L), 0, receivedWatermarks::add);
        assertThat(receivedWatermarks).hasSize(1);
        assertThat(receivedWatermarks)
                .map(watermark -> ((LongWatermark) watermark).getValue())
                .containsExactly(1L);

        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(2L), 0, receivedWatermarks::add);
        assertThat(receivedWatermarks).hasSize(2);
        assertThat(receivedWatermarks)
                .map(watermark -> ((LongWatermark) watermark).getValue())
                .containsExactly(1L, 2L);

        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(1L), 1, receivedWatermarks::add);
        assertThat(receivedWatermarks).hasSize(2);
        assertThat(receivedWatermarks)
                .map(watermark -> ((LongWatermark) watermark).getValue())
                .containsExactly(1L, 2L);

        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(3L), 1, receivedWatermarks::add);
        assertThat(receivedWatermarks).hasSize(3);
        assertThat(receivedWatermarks)
                .map(watermark -> ((LongWatermark) watermark).getValue())
                .containsExactly(1L, 2L, 3L);

        combiner.combineWatermark(
                watermarkDeclaration.newWatermark(1L), 1, receivedWatermarks::add);
        assertThat(receivedWatermarks).hasSize(4);
        assertThat(receivedWatermarks)
                .map(watermark -> ((LongWatermark) watermark).getValue())
                .containsExactly(1L, 2L, 3L, 2L);
    }
}
