package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClient;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClientFactory;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataFetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** The implementation of {@link SingleChannelDataFetcher} interface. */
public class SingleChannelDataFetcherImpl implements SingleChannelDataFetcher {

    private final List<SingleChannelDataClient> clientList;

    private final SingleChannelDataClientFactory clientFactory;

    private long currentSegmentId = 0L;

    public SingleChannelDataFetcherImpl(SingleChannelDataClientFactory clientFactory) {
        this.clientList = new ArrayList<>();
        this.clientFactory = clientFactory;
        setupClientList();
    }

    private void setupClientList(){
        SingleChannelDataClient localSingleChannelDataClient = clientFactory.createLocalSingleChannelDataClient();
        if(localSingleChannelDataClient != null){
            clientList.add(localSingleChannelDataClient);
        }
        if(clientFactory.hasDfsClient()){
            clientList.add(clientFactory.createDfsSingleChannelDataClient());
        }
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability>
            getNextBuffer(InputChannel inputChannel) throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailability = Optional.empty();
        for(SingleChannelDataClient client : clientList){
            bufferAndAvailability = client.getNextBuffer(
                    inputChannel,
                    currentSegmentId);
        }
        if(!bufferAndAvailability.isPresent()){
            return Optional.empty();
        }
        BufferAndAvailability bufferData = bufferAndAvailability.get();
        if (bufferData.buffer().getDataType() == Buffer.DataType.SEGMENT_EVENT) {
            EndOfSegmentEvent endOfSegmentEvent =
                    (EndOfSegmentEvent)
                            EventSerializer.fromSerializedEvent(
                                    bufferData.buffer().getNioBufferReadable(),
                                    Thread.currentThread().getContextClassLoader());
            checkState(endOfSegmentEvent.getSegmentId() == (currentSegmentId + 1L));
            currentSegmentId++;
            //bufferData.buffer().recycleBuffer();
            //return getNextBuffer(inputChannel);
        }
        return Optional.of(bufferData);
    }

    @Override
    public void close() throws IOException {
        for(SingleChannelDataClient client : clientList){
            client.close();
        }
    }
}
