/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.GeneralizedWatermark;
import org.apache.flink.api.common.eventtime.GeneralizedWatermarkDeclaration;
import org.apache.flink.api.common.typeutils.GeneralizedWatermarkTypeSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible for managing {@link WatermarkStatus} and aligning received {@link
 * GeneralizedWatermark} of all channels.
 */
@SuppressWarnings("unchecked")
public class GeneralizedWatermarkAligner {
    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * Array of current status of all input channels. Changes as watermarks & watermark statuses are
     * fed into the valve.
     */
    private final InputChannelStatus[] channelStatuses;

    /** The last watermark emitted from the valve. */
    private GeneralizedWatermark lastOutputWatermark;

    /** The last watermark status emitted from the valve. */
    private WatermarkStatus lastOutputWatermarkStatus;

    /** A heap-based priority queue to help find the minimum watermark. */
    private final GeneralizedHeapPriorityQueue<InputChannelStatus> alignedChannelStatuses;

    private final GeneralizedWatermarkDeclaration watermarkDeclaration;

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    public GeneralizedWatermarkAligner(
            int numInputChannels, GeneralizedWatermarkDeclaration watermarkDeclaration) {
        checkArgument(numInputChannels > 0);
        this.watermarkDeclaration = checkNotNull(watermarkDeclaration);
        this.channelStatuses = new InputChannelStatus[numInputChannels];
        this.alignedChannelStatuses = new GeneralizedHeapPriorityQueue<>(numInputChannels);
        for (int i = 0; i < numInputChannels; i++) {
            channelStatuses[i] = new InputChannelStatus();
            channelStatuses[i].watermark = watermarkDeclaration.getMinWatermarkSupplier().get();
            channelStatuses[i].watermarkStatus = WatermarkStatus.ACTIVE;
            markWatermarkAligned(channelStatuses[i]);
        }

        this.lastOutputWatermark = watermarkDeclaration.getMinWatermarkSupplier().get();
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link PushingAsyncDataInput.DataOutput#emitWatermark(GeneralizedWatermark)}} will
     * be called to process the new Watermark.
     *
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     */
    public <T extends Comparable<T>> void inputWatermark(
            GeneralizedWatermark watermark,
            int channelIndex,
            PushingAsyncDataInput.DataOutput<?> output)
            throws Exception {
        // ignore the input watermark if its input channel, or all input channels are idle (i.e.
        // overall the valve is idle).
        if (lastOutputWatermarkStatus.isActive()
                && channelStatuses[channelIndex].watermarkStatus.isActive()) {

            // if the input watermark's value is less than the last received watermark for its input
            // channel, ignore it also.
            if (watermark.compareTo(channelStatuses[channelIndex].watermark) > 0) {
                channelStatuses[channelIndex].watermark = watermark;

                if (channelStatuses[channelIndex].isWatermarkAligned) {
                    adjustAlignedChannelStatuses(channelStatuses[channelIndex]);
                } else if (watermark.compareTo(lastOutputWatermark) > 0) {
                    // previously unaligned input channels are now aligned if its watermark has
                    // caught up
                    markWatermarkAligned(channelStatuses[channelIndex]);
                }

                // now, attempt to find a new min watermark across all aligned channels
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        }
    }

    /**
     * Feed a {@link WatermarkStatus} into the valve. This may trigger the valve to output either a
     * new Watermark Status, for which {@link
     * PushingAsyncDataInput.DataOutput#emitWatermarkStatus(WatermarkStatus)} will be called, or a
     * new Watermark, for which {@link
     * PushingAsyncDataInput.DataOutput#emitWatermark(GeneralizedWatermark)}} will be called.
     *
     * @param watermarkStatus the watermark status to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark status belongs to (index
     *     starting from 0)
     */
    public void inputWatermarkStatus(
            WatermarkStatus watermarkStatus,
            int channelIndex,
            PushingAsyncDataInput.DataOutput<?> output)
            throws Exception {
        // only account for watermark status inputs that will result in a status change for the
        // input
        // channel
        if (watermarkStatus.isIdle() && channelStatuses[channelIndex].watermarkStatus.isActive()) {
            // handle active -> idle toggle for the input channel
            channelStatuses[channelIndex].watermarkStatus = WatermarkStatus.IDLE;

            // the channel is now idle, therefore not aligned
            markWatermarkUnaligned(channelStatuses[channelIndex]);

            // if all input channels of the valve are now idle, we need to output an idle stream
            // status from the valve (this also marks the valve as idle)
            if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

                // now that all input channels are idle and no channels will continue to advance its
                // watermark,
                // we should "flush" all watermarks across all channels; effectively, this means
                // emitting
                // the max watermark across all channels as the new watermark. Also, since we
                // already try to advance
                // the min watermark as channels individually become IDLE, here we only need to
                // perform the flush
                // if the watermark of the last active channel that just became idle is the current
                // min watermark.
                if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
                    findAndOutputMaxWatermarkAcrossAllChannels(output);
                }

                lastOutputWatermarkStatus = WatermarkStatus.IDLE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            } else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
                // if the watermark of the channel that just became idle equals the last output
                // watermark (the previous overall min watermark), we may be able to find a new
                // min watermark from the remaining aligned channels
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        } else if (watermarkStatus.isActive()
                && channelStatuses[channelIndex].watermarkStatus.isIdle()) {
            // handle idle -> active toggle for the input channel
            channelStatuses[channelIndex].watermarkStatus = WatermarkStatus.ACTIVE;

            // if the last watermark of the input channel, before it was marked idle, is still
            // larger than
            // the overall last output watermark of the valve, then we can set the channel to be
            // aligned already.
            if (channelStatuses[channelIndex].watermark.compareTo(lastOutputWatermark) >= 0) {
                markWatermarkAligned(channelStatuses[channelIndex]);
            }

            // if the valve was previously marked to be idle, mark it as active and output an active
            // stream
            // status because at least one of the input channels is now active
            if (lastOutputWatermarkStatus.isIdle()) {
                lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            }
        }
    }

    private void findAndOutputNewMinWatermarkAcrossAlignedChannels(
            PushingAsyncDataInput.DataOutput<?> output) throws Exception {
        boolean hasAlignedChannels = !alignedChannelStatuses.isEmpty();

        // we acknowledge and output the new overall watermark if it really is aggregated
        // from some remaining aligned channel, and is also larger than the last output watermark
        if (hasAlignedChannels
                && alignedChannelStatuses.peek().watermark.compareTo(lastOutputWatermark) > 0) {
            lastOutputWatermark = alignedChannelStatuses.peek().watermark;
            GeneralizedWatermarkTypeSerializer<GeneralizedWatermark<?>> watermarkTypeSerializer =
                    watermarkDeclaration.getWatermarkTypeSerializer();
            output.emitWatermark(watermarkTypeSerializer.copy(lastOutputWatermark));
        }
    }

    /**
     * Mark the {@link InputChannelStatus} as watermark-aligned and add it to the {@link
     * #alignedChannelStatuses}.
     *
     * @param inputChannelStatus the input channel status to be marked
     */
    private void markWatermarkAligned(InputChannelStatus inputChannelStatus) {
        if (!inputChannelStatus.isWatermarkAligned) {
            inputChannelStatus.isWatermarkAligned = true;
            inputChannelStatus.addTo(alignedChannelStatuses);
        }
    }

    /**
     * Mark the {@link InputChannelStatus} as watermark-unaligned and remove it from the {@link
     * #alignedChannelStatuses}.
     *
     * @param inputChannelStatus the input channel status to be marked
     */
    private void markWatermarkUnaligned(InputChannelStatus inputChannelStatus) {
        if (inputChannelStatus.isWatermarkAligned) {
            inputChannelStatus.isWatermarkAligned = false;
            inputChannelStatus.removeFrom(alignedChannelStatuses);
        }
    }

    /**
     * Adjust the {@link #alignedChannelStatuses} when an element({@link InputChannelStatus}) in it
     * was modified. The {@link #alignedChannelStatuses} is a priority queue, when an element in it
     * was modified, we need to adjust the element's position to ensure its priority order.
     *
     * @param inputChannelStatus the modified input channel status
     */
    private void adjustAlignedChannelStatuses(InputChannelStatus inputChannelStatus) {
        alignedChannelStatuses.adjustModifiedElement(inputChannelStatus);
    }

    private void findAndOutputMaxWatermarkAcrossAllChannels(
            PushingAsyncDataInput.DataOutput<?> output) throws Exception {
        GeneralizedWatermark minWatermark = watermarkDeclaration.getMinWatermarkSupplier().get();

        for (InputChannelStatus channelStatus : channelStatuses) {
            minWatermark =
                    channelStatus.watermark.compareTo(minWatermark) > 0
                            ? channelStatus.watermark
                            : minWatermark;
        }

        if (minWatermark.compareTo(lastOutputWatermark) > 0) {
            lastOutputWatermark = minWatermark;
            output.emitWatermark(lastOutputWatermark);
        }
    }

    /**
     * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
     * status, and whether or not the channel's current watermark is aligned with the overall
     * watermark output from the valve.
     *
     * <p>There are 2 situations where a channel's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the channel is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the channel hasn't
     *       caught up to the last output watermark from the valve yet.
     * </ul>
     *
     * <p>NOTE: This class implements {@link HeapPriorityQueue.HeapPriorityQueueElement} to be
     * managed by {@link #alignedChannelStatuses} to help find minimum watermark.
     */
    @VisibleForTesting
    protected static class InputChannelStatus
            implements GeneralizedHeapPriorityQueue.HeapPriorityQueueElement<InputChannelStatus> {
        protected GeneralizedWatermark watermark;
        protected WatermarkStatus watermarkStatus;
        protected boolean isWatermarkAligned;

        /**
         * This field holds the current physical index of this channel status when it is managed by
         * a {@link GeneralizedHeapPriorityQueue}.
         */
        private int heapIndex = GeneralizedHeapPriorityQueue.HeapPriorityQueueElement.NOT_CONTAINED;

        @Override
        public int getInternalIndex() {
            return heapIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.heapIndex = newIndex;
        }

        private void removeFrom(GeneralizedHeapPriorityQueue<InputChannelStatus> queue) {
            checkState(heapIndex != HeapPriorityQueue.HeapPriorityQueueElement.NOT_CONTAINED);
            queue.remove(this);
            setInternalIndex(GeneralizedHeapPriorityQueue.HeapPriorityQueueElement.NOT_CONTAINED);
        }

        private void addTo(GeneralizedHeapPriorityQueue<InputChannelStatus> queue) {
            checkState(heapIndex == HeapPriorityQueue.HeapPriorityQueueElement.NOT_CONTAINED);
            queue.add(this);
        }

        @Override
        public int compareTo(InputChannelStatus that) {
            return watermark.compareTo(that.watermark);
        }

        /**
         * Utility to check if at least one channel in a given array of input channels is active.
         */
        private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.watermarkStatus.isActive()) {
                    return true;
                }
            }
            return false;
        }
    }

    @VisibleForTesting
    protected InputChannelStatus getInputChannelStatus(int channelIndex) {
        Preconditions.checkArgument(
                channelIndex >= 0 && channelIndex < channelStatuses.length,
                "Invalid channel index. Number of input channels: " + channelStatuses.length);

        return channelStatuses[channelIndex];
    }
}
