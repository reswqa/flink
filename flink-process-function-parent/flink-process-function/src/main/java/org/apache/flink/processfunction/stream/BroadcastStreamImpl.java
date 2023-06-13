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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.processfunction.DataStream;
import org.apache.flink.processfunction.ExecutionEnvironmentImpl;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.BroadcastStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

/** Implementation for {@link BroadcastStream}. */
public class BroadcastStreamImpl<T> extends DataStream<T> implements BroadcastStream<T> {

    public BroadcastStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <K, T_OTHER, OUT> NonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        return null;
    }

    @Override
    public <T_OTHER, OUT> NonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        return null;
    }
}
