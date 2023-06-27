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

package org.apache.flink.processfunction.api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;

public abstract class ExecutionEnvironment {
    public static ExecutionEnvironment getExecutionEnvironment()
            throws ReflectiveOperationException {
        return (ExecutionEnvironment)
                Class.forName("org.apache.flink.processfunction.ExecutionEnvironmentImpl")
                        .getMethod("newInstance")
                        .invoke(null);
    }

    /** TODO: No watermark strategy atm. Revisit event-time supports later. */
    public abstract <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> fromSource(
            Source<OUT, ?, ?> source,
            WatermarkStrategy<OUT> timestampsAndWatermarks,
            String sourceName);

    public abstract void execute() throws Exception;

    /** TODO: Temporal method. Probably should not decide execution mode programmatically. */
    public abstract ExecutionEnvironment tmpSetRuntimeMode(RuntimeExecutionMode runtimeMode);

    /**
     * TODO: Temporal method. Probably should not set config programmatically. Just for testing in
     * PoC.
     */
    public abstract ExecutionEnvironment tmpWithConfiguration(Configuration configuration);
}
