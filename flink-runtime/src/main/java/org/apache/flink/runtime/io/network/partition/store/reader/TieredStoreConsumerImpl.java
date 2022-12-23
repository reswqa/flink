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

package org.apache.flink.runtime.io.network.partition.store.reader;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.TieredStoreConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The reader of Tiered Store. */
public class TieredStoreConsumerImpl implements TieredStoreConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreConsumerImpl.class);

    private final int subpartitionId;

    private final BufferAvailabilityListener availabilityListener;

    private final Object lock = new Object();

    private final SingleTierDataGate[] tierDataGates;

    private final SingleTierReader[] singleTierReaders;

    @Nullable
    @GuardedBy("lock")
    private Throwable failureCause = null;

    @GuardedBy("lock")
    private boolean isReleased = false;

    private int currentSegmentIndex = 0;

    private boolean hasSegmentFinished = true;

    private int viewIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = 0;

    private String taskName;

    public TieredStoreConsumerImpl(
            int subpartitionId,
            BufferAvailabilityListener availabilityListener,
            SingleTierDataGate[] tierDataGates,
            String taskName)
            throws IOException {
        checkArgument(tierDataGates.length > 0, "Empty tier transmitters.");

        this.subpartitionId = subpartitionId;
        this.availabilityListener = availabilityListener;
        this.tierDataGates = tierDataGates;
        this.singleTierReaders = new SingleTierReader[tierDataGates.length];
        createSingleTierReaders();
        this.taskName = taskName;
    }

    private void createSingleTierReaders() throws IOException {
        for (int i = 0; i < tierDataGates.length; i++) {
            singleTierReaders[i] =
                    tierDataGates[i].createSubpartitionTierReader(
                            subpartitionId, availabilityListener);
        }
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        try {
            synchronized (lock) {
                if (!findTierContainsNextSegment()) {
                    LOG.warn(
                            String.format(
                                    "Can not find a tier contains the next segment %d for subpartition %d.",
                                    currentSegmentIndex, subpartitionId));
                    LOG.debug(
                            "%%% {} Can not find a tier contains the next segment {} for subpartition {}.",
                            taskName, currentSegmentIndex, subpartitionId);
                    return null;
                }

                LOG.debug(
                        "%%% {} TieredStoreConsumerImpl is trying to getNextBuffer {}, {}",
                        taskName, currentSegmentIndex, viewIndexContainsCurrentSegment);

                BufferAndBacklog bufferAndBacklog =
                        singleTierReaders[viewIndexContainsCurrentSegment].getNextBuffer();

                if (bufferAndBacklog == null) {
                    LOG.debug("%%% {} TieredStoreConsumerImpl get null data.", taskName);
                }

                if (bufferAndBacklog != null) {
                    bufferAndBacklog.setNextDataType(Buffer.DataType.DATA_BUFFER);
                    hasSegmentFinished = bufferAndBacklog.isLastBufferInSegment();
                    LOG.debug(
                            "%%% {} TieredStoreConsumerImpl get bufferandbacklog, is SegmentFinished {}",
                            taskName, hasSegmentFinished);
                    if (hasSegmentFinished) {
                        currentSegmentIndex++;
                    }
                    bufferAndBacklog.setSequenceNumber(currentSequenceNumber);
                    currentSequenceNumber++;
                }
                if (bufferAndBacklog != null) {
                    if (bufferAndBacklog.buffer().getDataType()
                            != Buffer.DataType.SEGMENT_INFO_BUFFER) {
                        LOG.debug(
                                "### {} Buffer type, {}",
                                bufferAndBacklog.buffer().getDataType(),
                                taskName);
                    }
                }
                return bufferAndBacklog;
            }
        } catch (Throwable cause) {
            // release subpartition reader outside of lock to avoid deadlock.
            releaseInternal(cause);
            throw new IOException("Faild to get next buffer in reader.", cause);
        }
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            int numCreditsAvailable) {
        synchronized (lock) {
            // first scan all result subpartition views
            for (SingleTierReader singleTierReader : singleTierReaders) {
                singleTierReader.getAvailabilityAndBacklog(numCreditsAvailable);
            }
            if (findTierContainsNextSegment()) {
                LOG.debug(
                        "%%% find the subpartitionId {}, currentSegmentIndex {}, successful!",
                        subpartitionId, currentSegmentIndex);
                return singleTierReaders[viewIndexContainsCurrentSegment].getAvailabilityAndBacklog(
                        numCreditsAvailable);
            }
            LOG.debug(
                    "%%% find the subpartitionId {}, currentSegmentIndex {}, not finded",
                    subpartitionId, currentSegmentIndex);
            return new ResultSubpartitionView.AvailabilityWithBacklog(true, 0);
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        releaseInternal(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        findTierContainsNextSegment();
        return singleTierReaders[viewIndexContainsCurrentSegment]
                .unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            findTierContainsNextSegment();
            return singleTierReaders[viewIndexContainsCurrentSegment].getNumberOfQueuedBuffers();
        }
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private boolean findTierContainsNextSegment() {

        for (SingleTierReader singleTierReader : singleTierReaders) {
            singleTierReader.getAvailabilityAndBacklog(Integer.MAX_VALUE);
        }

        LOG.debug(
                "%%% {} Trying to find the segment index {} in which tier",
                taskName, currentSegmentIndex);

        if (!hasSegmentFinished) {
            LOG.debug("%%% {} segment not finished", taskName);
            return true;
        }

        for (int i = 0; i < tierDataGates.length; i++) {
            SingleTierDataGate tieredDataGate = tierDataGates[i];
            if (tieredDataGate.hasCurrentSegment(subpartitionId, currentSegmentIndex)) {
                LOG.debug(
                        "%%% {} Successfully find the segment index {} in tier {}",
                        taskName, currentSegmentIndex, i);
                viewIndexContainsCurrentSegment = i;
                return true;
            }
        }

        return false;
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        boolean[] toReleaseViews = new boolean[singleTierReaders.length];
        Arrays.fill(toReleaseViews, false);
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            failureCause = throwable;
            for (int i = 0; i < singleTierReaders.length; i++) {
                if (singleTierReaders[i] != null) {
                    toReleaseViews[i] = true;
                }
            }
        }
        for (int i = 0; i < toReleaseViews.length; i++) {
            if (toReleaseViews[i]) {
                try {
                    checkNotNull(singleTierReaders[i]).releaseAllResources();
                } catch (IOException ioException) {
                    throw new RuntimeException(
                            "Failed to release partition view resources.", ioException);
                }
            }
        }
    }
}
