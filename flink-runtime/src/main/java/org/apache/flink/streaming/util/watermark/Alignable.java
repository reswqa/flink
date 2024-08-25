package org.apache.flink.streaming.util.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;

/**
 * An interface used to represent the special {@link WatermarkDeclaration}s can create aligned
 * {@link Watermark}s, which need to be aligned when the operator receives them from inputs. Note
 * that this interface is currently only used for internal implementation.
 */
@Internal
public interface Alignable {

    /** Represents whether the created {@link Watermark} needs to be aligned. */
    boolean isAligned();
}
