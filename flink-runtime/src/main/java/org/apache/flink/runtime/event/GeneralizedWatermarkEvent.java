package org.apache.flink.runtime.event;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkElement;

import java.io.IOException;

/**
 * This event wraps the {@link GeneralizedWatermarkElement}, it is used in propagate {@link
 * Watermark} between shuffle components, and should not be visible to operators and functions.
 */
public class GeneralizedWatermarkEvent extends RuntimeEvent {

    private static final int TAG_LONG_GENERALIZED_WATERMARK = 0;
    private static final int TAG_BOOL_GENERALIZED_WATERMARK = 1;

    private GeneralizedWatermarkElement watermarkElement;
    private boolean isAligned = false;

    public GeneralizedWatermarkEvent() {}

    public GeneralizedWatermarkEvent(
            GeneralizedWatermarkElement watermarkElement, boolean isAligned) {
        this.watermarkElement = watermarkElement;
        this.isAligned = isAligned;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        // write watermark identifier
        out.writeUTF(watermarkElement.getWatermark().getIdentifier());

        // write watermark class tag
        // write watermark value
        if (watermarkElement.getWatermark() instanceof LongWatermark) {
            out.writeInt(TAG_LONG_GENERALIZED_WATERMARK);
            out.writeLong(((LongWatermark) watermarkElement.getWatermark()).getValue());
        } else if (watermarkElement.getWatermark() instanceof BoolWatermark) {
            out.writeInt(TAG_BOOL_GENERALIZED_WATERMARK);
            out.writeBoolean(((BoolWatermark) watermarkElement.getWatermark()).getValue());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported watermark type: " + watermarkElement.getClass());
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        // read watermark identifier
        String identifier = in.readUTF();

        // read watermark class tag
        int watermarkTypeTag = in.readInt();

        // read watermark value
        if (watermarkTypeTag == TAG_LONG_GENERALIZED_WATERMARK) {
            long value = in.readLong();
            this.watermarkElement =
                    new GeneralizedWatermarkElement(new LongWatermark(value, identifier));
        } else if (watermarkTypeTag == TAG_BOOL_GENERALIZED_WATERMARK) {
            boolean value = in.readBoolean();
            this.watermarkElement =
                    new GeneralizedWatermarkElement(new BoolWatermark(value, identifier));
        } else {
            throw new IllegalArgumentException("Unknown watermark class tag: " + watermarkTypeTag);
        }
    }

    public GeneralizedWatermarkElement getWatermarkElement() {
        return watermarkElement;
    }

    public boolean isAligned() {
        return isAligned;
    }
}
