package org.apache.flink.runtime.io.network.partition.store.dfs;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannelTest;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.DataFetcherState;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.DfsDataFetcher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.store.common.StoreReadWriteUtils.createBaseSubpartitionPath;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link DfsDataFetcher}. */
public class DfsDataFetcherTest {

    public static final int NUM_BUFFERS = 100;

    public static final int BUFFERS_IN_SEGMENT = 10;

    public static final int MEMORY_SEGMENT_SIZE = 128;

    private static final String TEST_STRING = "abcdefghijklmn";

    private static final int SEGMENT_ID = 0;

    private static final int CUR_SEQUENCE_NUM = 0;

    private DfsDataFetcher dfsDataFetcher;

    private NetworkBufferPool networkBufferPool;

    private LocalBufferPool localBufferPool;

    private JobID jobID = JobID.generate();

    private List<ResultPartitionID> resultPartitionIDS =
            new ArrayList<ResultPartitionID>() {
                {
                    add(new ResultPartitionID());
                }
            };

    private int inputGateIndex = 0;

    private int inputChannelIndex = 0;

    @TempDir public static java.nio.file.Path tempDataPath;

    private TemporaryFolder tmpFolder;

    @BeforeEach
    void before() throws Exception {
        resultPartitionIDS.add(new ResultPartitionID());
        networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, MEMORY_SEGMENT_SIZE);
        localBufferPool = new LocalBufferPool(networkBufferPool, NUM_BUFFERS);
        dfsDataFetcher = new DfsDataFetcher();
        dfsDataFetcher.setup(jobID, resultPartitionIDS, localBufferPool, inputGateIndex, "./");
        tmpFolder = TemporaryFolder.builder().build();
        tmpFolder.create();
    }

    @AfterEach
    void close() throws IOException {
        dfsDataFetcher.close();
        localBufferPool.lazyDestroy();
        networkBufferPool.destroy();
        dfsDataFetcher = null;
        localBufferPool = null;
        networkBufferPool = null;
    }

    @Test
    void testSetup() {
        assertThat(dfsDataFetcher.getAvailableBuffers()).hasSize(1);
        assertThat(dfsDataFetcher.getUsedBuffers()).hasSize(0);
        assertThat(dfsDataFetcher.getCurSequenceNumber()).isEqualTo(Integer.MIN_VALUE);
        assertThat(dfsDataFetcher.getCurrentChannel()).isEqualTo(Optional.empty());
        assertThat(dfsDataFetcher.getState()).isNotEqualTo(DataFetcherState.RUNNING);
    }

    @Test
    void testResolveSegmentInfo() {
        //dfsDataFetcher.setBaseSubpartitionPath(getTempStorePathDir());
        //NetworkBuffer info1 = createTempSegmentInfo(0, 0);
        //assertThat(dfsDataFetcher.resolveSegmentInfo(info1,)).isTrue();
        //assertThat(dfsDataFetcher.getCurSegmentId()).isEqualTo(0);
        //assertThat(dfsDataFetcher.getCurSequenceNumber()).isEqualTo(0);
        //NetworkBuffer info2 = createTempSegmentInfo(6, 7);
        //assertThat(dfsDataFetcher.resolveSegmentInfo(info2)).isTrue();
        //assertThat(dfsDataFetcher.getCurSegmentId()).isEqualTo(6);
        //assertThat(dfsDataFetcher.getCurSequenceNumber()).isEqualTo(7);
        //NetworkBuffer info3 = createTempSegmentInfo(-1, 100);
        //assertThat(dfsDataFetcher.resolveSegmentInfo(info3)).isFalse();
        //assertThat(dfsDataFetcher.getCurSegmentId()).isEqualTo(6);
        //assertThat(dfsDataFetcher.getCurSequenceNumber()).isEqualTo(7);
    }

    @Test
    void testResolveDfsBuffer() throws Exception {
        NetworkBuffer info = createTempSegmentInfo(SEGMENT_ID, CUR_SEQUENCE_NUM);
        Path path = new Path(getTempStorePathDir() + "/seg-" + SEGMENT_ID);
        createTempSegmentFile(path);
        dfsDataFetcher.setBaseSubpartitionPath(getTempStorePathDir());
        //dfsDataFetcher.resolveSegmentInfo(info);
        FSDataInputStream currentInputStream =
                path.getFileSystem().open(dfsDataFetcher.getCurrentDataPath());
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            assertThat(currentInputStream.available()).isGreaterThan(0);
            InputChannel.BufferAndAvailability bufferAndAvailability =
                    dfsDataFetcher.resolveDfsBuffer(currentInputStream);
            assertThat(bufferAndAvailability.moreAvailable()).isTrue();
            assertThat(bufferAndAvailability.getSequenceNumber()).isEqualTo(i);
            NetworkBuffer buffer = (NetworkBuffer) bufferAndAvailability.buffer();
            byte[] data = new byte[buffer.getSize()];
            buffer.getBytes(0, data, 0, buffer.getSize());
            String result = new String(data, StandardCharsets.UTF_8);
            assertThat(result).isEqualTo(TEST_STRING);
        }
        assertThat(currentInputStream.available()).isEqualTo(0);
        currentInputStream.close();
    }

    @Test
    void testRun() throws Exception {
        String tempSegmentPath =
                createBaseSubpartitionPath(jobID, resultPartitionIDS.get(0), inputGateIndex, "./", false);
        createTempSegmentFile(new Path(tempSegmentPath, "/seg-" + SEGMENT_ID));
        setSegmentInfoToDfsDataFetcher();
        while (dfsDataFetcher.getUsedBuffers().size() < BUFFERS_IN_SEGMENT) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        ArrayDeque<InputChannel.BufferAndAvailability> usedBuffers =
                dfsDataFetcher.getUsedBuffers();
        assertThat(usedBuffers).hasSize(BUFFERS_IN_SEGMENT);
        deleteTempSegmentFile(new Path(tempSegmentPath));
    }

    @Test
    void testGetNextBufferInBlockingMode() throws Exception {
        String tempSegmentPath =
                createBaseSubpartitionPath(jobID, resultPartitionIDS.get(0), inputGateIndex, "./", false);
        createTempSegmentFile(new Path(tempSegmentPath, "/seg-" + SEGMENT_ID));
        setSegmentInfoToDfsDataFetcher();
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            Optional<InputChannel.BufferAndAvailability> nextBuffer =
                    dfsDataFetcher.getNextBuffer(true);
            assertThat(nextBuffer.isPresent()).isTrue();
            assertThat(nextBuffer.get().getSequenceNumber()).isEqualTo(i);
            assertThat(nextBuffer.get().buffersInBacklog()).isEqualTo(0);
            assertThat(nextBuffer.get().moreAvailable()).isTrue();
            assertThat(nextBuffer.get().morePriorityEvents()).isFalse();
            assertThat(nextBuffer.get().hasPriority()).isFalse();
            NetworkBuffer buffer = (NetworkBuffer) nextBuffer.get().buffer();
            byte[] data = new byte[buffer.getSize()];
            buffer.getBytes(0, data, 0, buffer.getSize());
            String result = new String(data, StandardCharsets.UTF_8);
            assertThat(result).isEqualTo(TEST_STRING);
        }
        assertThat(dfsDataFetcher.getUsedBuffers()).hasSize(0);
        assertThat(dfsDataFetcher.getAvailableBuffers()).hasSize(0);
        deleteTempSegmentFile(new Path(tempSegmentPath));
    }

    @Test
    void testGetNextBufferInNonBlockingMode() throws Exception {
        String tempSegmentPath =
                createBaseSubpartitionPath(jobID, resultPartitionIDS.get(0), inputGateIndex, "./", false);
        createTempSegmentFile(new Path(tempSegmentPath, "/seg-" + SEGMENT_ID));
        setSegmentInfoToDfsDataFetcher();
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            Optional<InputChannel.BufferAndAvailability> nextBuffer = Optional.empty();
            while (!nextBuffer.isPresent()) {
                nextBuffer = dfsDataFetcher.getNextBuffer(false);
            }
            assertThat(nextBuffer.get().getSequenceNumber()).isEqualTo(i);
            assertThat(nextBuffer.get().buffersInBacklog()).isEqualTo(0);
            assertThat(nextBuffer.get().moreAvailable()).isTrue();
            assertThat(nextBuffer.get().morePriorityEvents()).isFalse();
            assertThat(nextBuffer.get().hasPriority()).isFalse();
            NetworkBuffer buffer = (NetworkBuffer) nextBuffer.get().buffer();
            byte[] data = new byte[buffer.getSize()];
            buffer.getBytes(0, data, 0, buffer.getSize());
            String result = new String(data, StandardCharsets.UTF_8);
            assertThat(result).isEqualTo(TEST_STRING);
        }
        assertThat(dfsDataFetcher.getUsedBuffers()).hasSize(0);
        assertThat(dfsDataFetcher.getAvailableBuffers()).hasSize(0);
        deleteTempSegmentFile(new Path(tempSegmentPath));
    }

    private void setSegmentInfoToDfsDataFetcher() throws Exception {
        NetworkBuffer info = createTempSegmentInfo(SEGMENT_ID, CUR_SEQUENCE_NUM);
        SingleInputGate inputGate =
                new SingleInputGateBuilder().setBufferPoolFactory(localBufferPool).build();
        RemoteInputChannel inputChannel =
                InputChannelBuilder.newBuilder()
                        .setConnectionManager(
                                new RemoteInputChannelTest.TestVerifyConnectionManager(
                                        new RemoteInputChannelTest
                                                .TestVerifyPartitionRequestClient()))
                        .setChannelIndex(inputChannelIndex)
                        .buildRemoteChannel(inputGate);
        dfsDataFetcher.setSegmentInfo(inputChannel, info);
    }

    public static NetworkBuffer createTempSegmentInfo(int segmentId, int curSequenceNumber) {
        ByteBuffer headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.clear();
        headerBuffer.putInt(segmentId);
        headerBuffer.putInt(curSequenceNumber);
        headerBuffer.flip();
        return new NetworkBuffer(
                MemorySegmentFactory.wrap(headerBuffer.array()),
                BufferRecycler.DummyBufferRecycler.INSTANCE,
                Buffer.DataType.SEGMENT_EVENT);
    }

    public static void createTempSegmentFile(Path path) throws Exception {
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        FSDataOutputStream outputStream = fileSystem.create(path, FileSystem.WriteMode.OVERWRITE);
        for (int i = 0; i < BUFFERS_IN_SEGMENT; ++i) {
            writeSingleBuffer(outputStream);
        }
        outputStream.close();
    }

    public static void deleteTempSegmentFile(Path path) throws Exception {
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        fileSystem.delete(path, true);
    }

    private static void writeSingleBuffer(FSDataOutputStream outputStream) throws IOException {
        // 1. Build the data buffer
        byte[] stringData = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        NetworkBuffer dataBuffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(stringData.length),
                        BufferRecycler.DummyBufferRecycler.INSTANCE);
        dataBuffer.writeBytes(stringData);
        // 2. Build the data header buffer
        ByteBuffer headerBuffer = ByteBuffer.wrap(new byte[8]);
        headerBuffer.order(ByteOrder.nativeOrder());
        BufferReaderWriterUtil.setByteChannelBufferHeader(dataBuffer, headerBuffer);
        outputStream.write(headerBuffer.array());
        outputStream.write(dataBuffer.getMemorySegment().getArray());
        dataBuffer.recycleBuffer();
    }

    private String getTempStorePathDir() {
        return tmpFolder.getRoot().getPath();
    }
}
