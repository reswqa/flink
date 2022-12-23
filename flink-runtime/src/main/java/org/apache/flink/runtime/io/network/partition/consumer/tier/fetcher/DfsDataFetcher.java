package org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.throwCorruptDataException;
import static org.apache.flink.runtime.io.network.partition.store.common.StoreReadWriteUtils.createBaseSubpartitionPath;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The TieredStoreDataFetcher for DFS data. */
public class DfsDataFetcher implements Runnable, TieredStoreDataFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(DfsDataFetcher.class);

    private final AtomicBoolean isSegmentFinished = new AtomicBoolean();

    private static final int MINIMUM_BUFFER_NUMBER = 1;

    private final ArrayDeque<MemorySegment> availableBuffers = new ArrayDeque<>();

    private final ArrayDeque<BufferAndAvailability> usedBuffers = new ArrayDeque<>();

    private BufferPool bufferPool;

    private volatile DataFetcherState currentState = DataFetcherState.CLOSED;

    private InputChannel currentChannel;

    private Path currentPath;

    private int curSegmentId = Integer.MIN_VALUE;

    private int curSequenceNumber = Integer.MIN_VALUE;

    private ExecutorService dataFetcher;

    private ByteBuffer headerBuffer;

    private ByteBuffer dataBuffer;

    private String baseSubpartitionPath;

    private JobID jobID;

    List<ResultPartitionID> resultPartitionIDs;

    private int subpartitionIndex;

    private String baseDfsPath;

    @Override
    public void setup(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            BufferPool bufferPool,
            int subpartitionIndex,
            String baseDfsPath)
            throws InterruptedException, IOException {
        this.bufferPool = bufferPool;
        headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.order(ByteOrder.nativeOrder());
        for (int i = 0; i < MINIMUM_BUFFER_NUMBER; ++i) {
            availableBuffers.add(bufferPool.requestMemorySegmentBlocking());
        }
        this.currentState = DataFetcherState.WAITING;
        dataFetcher = Executors.newSingleThreadExecutor();
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
    }

    @Override
    public void close() throws IOException {
        if (dataFetcher != null) {
            dataFetcher.shutdown();
        }
        availableBuffers.forEach(MemorySegment::free);
        availableBuffers.clear();
        usedBuffers.clear();
        currentState = DataFetcherState.CLOSED;
    }

    @Override
    public void setSegmentInfo(InputChannel inputChannel, Buffer segmentInfo)
            throws InterruptedException, IOException {
        checkNotNull(inputChannel);
        checkNotNull(segmentInfo);
        checkNotNull(resultPartitionIDs);
        if (resolveSegmentInfo(segmentInfo, inputChannel)) {
            currentChannel = inputChannel;
            synchronized (isSegmentFinished) {
                isSegmentFinished.set(false);
            }
            currentState = DataFetcherState.RUNNING;
            dataFetcher.execute(this);
        }
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer(Boolean isBlocking)
            throws InterruptedException {
        BufferAndAvailability result;
        while (true) {
            synchronized (isSegmentFinished) {
                result = usedBuffers.poll();
                if (result != null) {
                    break;
                } else {
                    if (isSegmentFinished.get()) {
                        break;
                    } else {
                        isSegmentFinished.wait();
                    }
                }
            }
        }
        return result == null ? Optional.empty() : Optional.of(result);
    }

    @Override
    public DataFetcherState getState() {
        return currentState;
    }

    @Override
    public void setState(DataFetcherState state) {
        currentState = state;
    }

    @Override
    public Optional<InputChannel> getCurrentChannel() {
        return currentChannel == null ? Optional.empty() : Optional.of(currentChannel);
    }

    @Override
    public void run() {
        try (FSDataInputStream currentInputStream = currentPath.getFileSystem().open(currentPath)) {
            while (currentInputStream.available() != 0) {
                BufferAndAvailability bufferAndAvailability = resolveDfsBuffer(currentInputStream);
                if (bufferAndAvailability == null) {
                    continue;
                }
                synchronized (isSegmentFinished) {
                    usedBuffers.add(bufferAndAvailability);
                    isSegmentFinished.notify();
                }
            }
            synchronized (isSegmentFinished) {
                isSegmentFinished.set(true);
                isSegmentFinished.notify();
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public boolean resolveSegmentInfo(Buffer segmentInfo, InputChannel inputChannel) throws IOException {
        int segmentIdResult;
        int sequenceNumberResult;
        boolean isBroadcastOnly;
        ByteBuf byteBuf = segmentInfo.asByteBuf();
        try {
            isBroadcastOnly = byteBuf.getInt(0) == 1;
            segmentIdResult = byteBuf.getInt(4);
            sequenceNumberResult = byteBuf.getInt(8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve segment info.", e);
        }
        segmentInfo.recycleBuffer();
        if (segmentIdResult >= 0 && sequenceNumberResult >= 0) {
            curSegmentId = segmentIdResult;
            curSequenceNumber = sequenceNumberResult;
            baseSubpartitionPath =
                    createBaseSubpartitionPath(
                            jobID,
                            resultPartitionIDs.get(inputChannel.getChannelIndex()),
                            subpartitionIndex,
                            baseDfsPath,
                            isBroadcastOnly);
            currentPath = new Path(baseSubpartitionPath, "/seg-" + curSegmentId);
            try {
                checkState(currentPath.getFileSystem().exists(currentPath));
                LOG.info(
                        "### DfsDataFetcher resolve successfully, curSegmentId: {}, curSequenceNumber: {}, is exist: {}, is broadcast {}, path: {}",
                        curSegmentId,
                        curSequenceNumber,
                        currentPath.getFileSystem().exists(currentPath),
                        isBroadcastOnly,
                        currentPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    public BufferAndAvailability resolveDfsBuffer(FSDataInputStream inputStream)
            throws InterruptedException, IOException {
        LOG.debug("### START RUNNING! resolve1 " + currentPath);
        if (availableBuffers.isEmpty()) {
            LOG.debug("### START RUNNING! resolve2 " + currentPath);
            MemorySegment memorySegment = bufferPool.requestMemorySegmentBlocking();
            if (memorySegment != null) {
                availableBuffers.add(memorySegment);
            }
        }
        if (availableBuffers.isEmpty()) {
            return null;
        }
        LOG.debug("### START RUNNING! resolve3 " + currentPath);
        MemorySegment memorySegment = availableBuffers.poll();
        LOG.debug("### START RUNNING! resolve4 " + currentPath);
        Buffer buffer = checkNotNull(readFromInputStream(memorySegment, inputStream));
        return new BufferAndAvailability(
                buffer, Buffer.DataType.DATA_BUFFER, 0, curSequenceNumber++);
    }

    private Buffer readFromInputStream(MemorySegment memorySegment, FSDataInputStream inputStream)
            throws IOException {
        headerBuffer.clear();
        int bufferHeaderResult = inputStream.read(headerBuffer.array());
        if (bufferHeaderResult == -1) {
            return null;
        }
        final BufferHeader header;
        try {
            header = parseBufferHeader(headerBuffer);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            // buffer underflow if header buffer is undersized
            // IllegalArgumentException if size is outside memory segment size
            throwCorruptDataException();
            return null; // silence compiler
        }
        dataBuffer = ByteBuffer.wrap(new byte[header.getLength()]);
        int dataBufferResult = inputStream.read(dataBuffer.array(), 0, header.getLength());
        if (dataBufferResult == -1) {
            return null;
        }
        Buffer.DataType dataType = header.getDataType();
        memorySegment.put(0, dataBuffer.array(), 0, header.getLength());
        return new NetworkBuffer(
                memorySegment, bufferPool, dataType, header.isCompressed(), header.getLength());
    }

    @VisibleForTesting
    public Path getCurrentDataPath() {
        return currentPath;
    }

    @VisibleForTesting
    public int getCurSequenceNumber() {
        return curSequenceNumber;
    }

    @VisibleForTesting
    public int getCurSegmentId() {
        return curSegmentId;
    }

    @VisibleForTesting
    public ArrayDeque<MemorySegment> getAvailableBuffers() {
        return availableBuffers;
    }

    @VisibleForTesting
    public ArrayDeque<BufferAndAvailability> getUsedBuffers() {
        return usedBuffers;
    }

    @VisibleForTesting
    public void setBaseSubpartitionPath(String baseSubpartitionPath) {
        this.baseSubpartitionPath = baseSubpartitionPath;
    }
}
