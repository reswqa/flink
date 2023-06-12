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

package org.apache.flink.processfunction.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.processfunction.DataStream;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.BroadcastStream;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.util.function.ConsumerFunction;

/** Implementation for {@link KeyedPartitionStream}. */
public class KeyedPartitionStreamImpl<K, V> extends DataStream<V>
        implements KeyedPartitionStream<K, V> {

    /**
     * The key selector that can get the key by which the stream if partitioned from the elements.
     */
    private final KeySelector<V, K> keySelector;

    /** The type of the key by which the stream is partitioned. */
    private final TypeInformation<K> keyType;

    public KeyedPartitionStreamImpl(DataStream<V> dataStream, KeySelector<V, K> keySelector) {
        this(
                dataStream,
                keySelector,
                TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType()));
    }

    public KeyedPartitionStreamImpl(
            DataStream<V> dataStream, KeySelector<V, K> keySelector, TypeInformation<K> keyType) {
        this(
                dataStream,
                new PartitionTransformation<>(
                        dataStream.getTransformation(),
                        new KeyGroupStreamPartitioner<>(
                                keySelector,
                                StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
                keySelector,
                keyType);
    }

    public KeyedPartitionStreamImpl(
            DataStream<V> dataStream,
            PartitionTransformation<V> partitionTransformation,
            KeySelector<V, K> keySelector,
            TypeInformation<K> keyType) {
        super(dataStream.getEnvironment(), partitionTransformation);
        this.keySelector = keySelector;
        this.keyType = keyType;
    }

    @Override
    public <OUT> KeyedPartitionStream<K, OUT> process(
            SingleStreamProcessFunction<V, OUT> processFunction) {
        return null;
    }

    @Override
    public <OUT1, OUT2> TwoOutputStreams<K, OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<V, OUT1, OUT2> processFunction) {
        return null;
    }

    @Override
    public <T_OTHER, OUT> KeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        return null;
    }

    @Override
    public <T_OTHER, OUT> KeyedPartitionStream<K, OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        return null;
    }

    @Override
    public GlobalStream<V> coalesce() {
        return null;
    }

    @Override
    public <NEW_KEY> KeyedPartitionStream<NEW_KEY, V> keyBy(KeySelector<V, NEW_KEY> keySelector) {
        // Create a new keyed stream with different key selector.
        return new KeyedPartitionStreamImpl<>(this, keySelector);
    }

    @Override
    public NonKeyedPartitionStream<V> shuffle() {
        return null;
    }

    @Override
    public BroadcastStream<V> broadcast() {
        return null;
    }

    @Override
    public void tmpToConsumerSink(ConsumerFunction<V> consumer) {}
}
