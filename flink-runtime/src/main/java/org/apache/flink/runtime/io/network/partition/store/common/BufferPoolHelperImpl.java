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

package org.apache.flink.runtime.io.network.partition.store.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * All buffers of Tiered Store are acquired from this {@link BufferPoolHelperImpl}. The buffers
 * mainly include two types, the first is the memory buffers and the second is the cached buffers.
 * If the memory buffer usage or the cached buffer usage exceed the ratio, no more buffers can be
 * acquired. If the total buffer usage exceeds the flush ratio, cached buffers will be flushed to
 * the corresponding disk files or DFS files to release the cached buffers.
 */
public class BufferPoolHelperImpl implements BufferPoolHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BufferPoolHelperImpl.class);

    private static final int ALL_SUBPARTITIONS_INDEX = -1;

    private final BufferPool bufferPool;

    private final float bufferInMemoryRatio;

    private final float flushBufferRatio;

    private final float triggerFlushRatio;

    private int numTotalBuffers;

    private int numInMemoryMaxBuffers;

    private int numStopNotifyFlushBuffers;

    private int numTriggerFlushBuffers;

    private final Queue<CompletableFuture<Void>> requestingBuffersQueue = new LinkedList<>();

    private final Map<SubpartitionTier, SubpartitionCachedBuffersCounter>
            subpartitionCachedBuffersMap = new HashMap<>();

    private final AtomicInteger numInMemoryBuffers = new AtomicInteger(0);

    private final AtomicInteger numTotalCacheBuffers = new AtomicInteger(0);

    private Queue<SubpartitionCachedBuffersCounter> subpartitionCachedBuffersCounters =
            new PriorityQueue<>();

    private CompletableFuture<Void> isTriggeringFlush = FutureUtils.completedVoidFuture();

    private static final int poolSizeCheckInterval = 500;

    private final ScheduledExecutorService poolSizeChecker =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("tiered-store-buffer-pool-checker"));

    private String taskName;

    public BufferPoolHelperImpl(
            BufferPool bufferPool,
            float bufferInMemoryRatio,
            float flushBufferRatio,
            float triggerFlushRatio,
            String taskName) {

        this.bufferPool = bufferPool;
        this.bufferInMemoryRatio = bufferInMemoryRatio;
        this.flushBufferRatio = flushBufferRatio;
        this.triggerFlushRatio = triggerFlushRatio;
        this.numTotalBuffers = this.bufferPool.getNumBuffers();

        checkState(flushBufferRatio < triggerFlushRatio);

        calculateNumBuffersLimit();

        if (poolSizeCheckInterval > 0) {
            poolSizeChecker.scheduleAtFixedRate(
                    () -> {
                        int newSize = this.bufferPool.getNumBuffers();
                        boolean needCheckFlush = numTotalBuffers > newSize;
                        if (numTotalBuffers != newSize) {
                            numTotalBuffers = newSize;
                            calculateNumBuffersLimit();
                            if (needCheckFlush) {
                                checkNeedFlushCachedBuffers();
                            }
                        }
                    },
                    poolSizeCheckInterval,
                    poolSizeCheckInterval,
                    TimeUnit.MILLISECONDS);
        }
        this.taskName = taskName;
    }

    public BufferPoolHelperImpl(
            BufferPool bufferPool,
            float bufferInMemoryRatio,
            float flushBufferRatio,
            float triggerFlushRatio) {
        this(bufferPool, bufferInMemoryRatio, flushBufferRatio, triggerFlushRatio, "Null");
    }

    @Override
    public int numPoolSize() {
        return bufferPool.getNumBuffers();
    }

    @Override
    public void registerSubpartitionTieredManager(
            int subpartitionId,
            TieredStoreMode.TieredType tieredType,
            NotifyFlushListener notifyFlushListener) {
        SubpartitionTier subpartitionTier = new SubpartitionTier(subpartitionId, tieredType);
        SubpartitionCachedBuffersCounter cachedBuffersCounter =
                new SubpartitionCachedBuffersCounter(subpartitionTier, notifyFlushListener);
        subpartitionCachedBuffersMap.put(subpartitionTier, cachedBuffersCounter);
        subpartitionCachedBuffersCounters.add(cachedBuffersCounter);
    }

    @Override
    public void registerSubpartitionTieredManager(
            TieredStoreMode.TieredType tieredType, NotifyFlushListener notifyFlushListener) {
        registerSubpartitionTieredManager(ALL_SUBPARTITIONS_INDEX, tieredType, notifyFlushListener);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking(
            int subpartitionId, TieredStoreMode.TieredType tieredType, boolean isInMemory) {
        try {
            if (!isInMemory) {
                return requestMemorySegmentFromPool(subpartitionId, tieredType, isInMemory);
            }

            checkState(tieredType == TieredStoreMode.TieredType.LOCAL);
            if (numInMemoryBuffers.get() < numInMemoryMaxBuffers) {
                return requestMemorySegmentFromPool(subpartitionId, tieredType, isInMemory);
            } else {
                CompletableFuture<Void> isInMemBuffersEnough = new CompletableFuture<>();
                requestingBuffersQueue.add(isInMemBuffersEnough);
                return isInMemBuffersEnough
                        .thenApply(
                                ignore -> {
                                    try {
                                        return requestMemorySegmentFromPool(
                                                subpartitionId, tieredType, isInMemory);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .get();
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to request memory segment from buffer pool.", e);
        }
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking(
            TieredStoreMode.TieredType tieredType, boolean isInMemory) {
        return requestMemorySegmentBlocking(ALL_SUBPARTITIONS_INDEX, tieredType, isInMemory);
    }

    @Override
    public void recycleBuffer(
            int subpartitionId,
            MemorySegment buffer,
            TieredStoreMode.TieredType tieredType,
            boolean isInMemory) {
        bufferPool.recycle(buffer);
        if (isInMemory) {
            checkState(tieredType == TieredStoreMode.TieredType.LOCAL);
            decInMemoryBuffer();
            if (numInMemoryBuffers.get() < numInMemoryMaxBuffers) {
                CompletableFuture<Void> requestingBuffer = requestingBuffersQueue.poll();
                if (requestingBuffer != null) {
                    requestingBuffer.complete(null);
                }
            }
        } else {
            decCachedBuffers(subpartitionId, tieredType);
        }
    }

    @Override
    public void recycleBuffer(
            MemorySegment buffer, TieredStoreMode.TieredType tieredType, boolean isInMemory) {
        recycleBuffer(ALL_SUBPARTITIONS_INDEX, buffer, tieredType, isInMemory);
    }

    @Override
    public int numCachedBuffers() {
        return numTotalCacheBuffers.get();
    }

    @Override
    public void checkNeedFlushCachedBuffers() {
        LOG.debug("{} is checking", taskName);
        int availableBuffers = bufferPool.getNetworkBufferPoolAvailableBuffers();
        checkState(availableBuffers >= 0);
        int totalBuffers = bufferPool.getNetworkBufferPoolTotalBuffers();
        checkState(totalBuffers > 0);
        double availableRatio = availableBuffers * 1.0 / totalBuffers;
        if ((numTotalCacheBuffers.get() + numInMemoryBuffers.get() < numTriggerFlushBuffers)
                        && (numTotalCacheBuffers.get() < 64) && availableRatio < 0.8
                || !isTriggeringFlush.isDone()) {
            LOG.debug("{} is checking1", taskName);
            return;
        }

        isTriggeringFlush = new CompletableFuture<>();
        sortToFlushSubpartitions();
        LOG.debug("{} is checking2", taskName);
        notifySubpartitionFlush();
        isTriggeringFlush.complete(null);
    }

    @Override
    public void close() {
        poolSizeChecker.shutdown();
    }

    private MemorySegment requestMemorySegmentFromPool(
            int subpartitionId, TieredStoreMode.TieredType tieredType, boolean isInMemory)
            throws IOException {
        MemorySegment requestedBuffer;
        try {
            requestedBuffer = bufferPool.requestMemorySegmentBlocking();
        } catch (Throwable throwable) {
            throw new IOException("Failed to request memory segments.", throwable);
        }
        if (isInMemory) {
            checkState(tieredType == TieredStoreMode.TieredType.LOCAL);
            incInMemoryBuffer();
        } else {
            incCachedBuffers(subpartitionId, tieredType);
            checkNeedFlushCachedBuffers();
        }
        return checkNotNull(requestedBuffer);
    }

    private void calculateNumBuffersLimit() {
        synchronized (BufferPoolHelperImpl.class) {
            // If the buffer pool only has one buffer, the in memory buffer and cached buffer use
            // the buffer in FIFO order.
            numInMemoryMaxBuffers = Math.max(1, (int) (numTotalBuffers * bufferInMemoryRatio));
            numStopNotifyFlushBuffers =
                    (int) (numTotalBuffers * (triggerFlushRatio - flushBufferRatio));
            numTriggerFlushBuffers = Math.max(1, (int) (numTotalBuffers * triggerFlushRatio));
        }
    }

    private void sortToFlushSubpartitions() {
        subpartitionCachedBuffersCounters = new PriorityQueue<>(subpartitionCachedBuffersCounters);
    }

    private void notifySubpartitionFlush() {
        int numMaxNotified = subpartitionCachedBuffersCounters.size();
        LOG.debug("{} is checking3", taskName);
        while ((numTotalCacheBuffers.get() + numInMemoryBuffers.get() >= numStopNotifyFlushBuffers
                && numMaxNotified > 0)) {
            LOG.debug("{} is checking4", taskName);
            SubpartitionCachedBuffersCounter buffersCounter =
                    checkNotNull(subpartitionCachedBuffersCounters.poll());
            LOG.debug(
                    "{} is checking5, Tier is {}, Subpartition Id is {} , num caches {}",
                    taskName,
                    buffersCounter.subpartitionTier.tieredType,
                    buffersCounter.subpartitionTier.subpartitionId,
                    buffersCounter.numCachedBuffers.get());
            buffersCounter.getNotifyFlushListener().notifyFlushCachedBuffers();
            subpartitionCachedBuffersCounters.add(buffersCounter);
            numMaxNotified--;
        }
    }

    void incCachedBuffers(int subpartitionId, TieredStoreMode.TieredType tieredType) {
        numTotalCacheBuffers.incrementAndGet();
        checkNotNull(
                        subpartitionCachedBuffersMap.get(
                                new SubpartitionTier(subpartitionId, tieredType)))
                .incNumCachedBuffers();
    }

    void decCachedBuffers(int subpartitionId, TieredStoreMode.TieredType tieredType) {
        numTotalCacheBuffers.decrementAndGet();
        checkNotNull(
                        subpartitionCachedBuffersMap.get(
                                new SubpartitionTier(subpartitionId, tieredType)))
                .decNumCachedBuffers();
    }

    void incInMemoryBuffer() {
        numInMemoryBuffers.incrementAndGet();
    }

    void decInMemoryBuffer() {
        numInMemoryBuffers.decrementAndGet();
    }

    private static class SubpartitionTier {

        private final int subpartitionId;

        private final TieredStoreMode.TieredType tieredType;

        public SubpartitionTier(int subpartition, TieredStoreMode.TieredType tieredType) {
            this.subpartitionId = subpartition;
            this.tieredType = tieredType;
        }

        public int getSubpartitionId() {
            return subpartitionId;
        }

        public TieredStoreMode.TieredType getTieredType() {
            return tieredType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubpartitionTier that = (SubpartitionTier) o;
            return subpartitionId == that.subpartitionId && tieredType == that.tieredType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subpartitionId, tieredType);
        }
    }

    private static class SubpartitionCachedBuffersCounter
            implements Comparable<SubpartitionCachedBuffersCounter> {

        SubpartitionTier subpartitionTier;

        private final NotifyFlushListener notifyFlushListener;

        private final AtomicInteger numCachedBuffers = new AtomicInteger(0);

        public SubpartitionCachedBuffersCounter(
                SubpartitionTier subpartitionTier, NotifyFlushListener notifyFlushListener) {
            this.subpartitionTier = subpartitionTier;
            this.notifyFlushListener = notifyFlushListener;
        }

        public int getSubpartitionId() {
            return subpartitionTier.getSubpartitionId();
        }

        public TieredStoreMode.TieredType getTieredType() {
            return subpartitionTier.getTieredType();
        }

        int numCachedBuffers() {
            return numCachedBuffers.get();
        }

        void incNumCachedBuffers() {
            numCachedBuffers.incrementAndGet();
        }

        public NotifyFlushListener getNotifyFlushListener() {
            return notifyFlushListener;
        }

        void decNumCachedBuffers() {
            numCachedBuffers.decrementAndGet();
        }

        @Override
        public int compareTo(BufferPoolHelperImpl.SubpartitionCachedBuffersCounter that) {
            return -1 * Integer.compare(numCachedBuffers(), that.numCachedBuffers());
        }
    }
}
