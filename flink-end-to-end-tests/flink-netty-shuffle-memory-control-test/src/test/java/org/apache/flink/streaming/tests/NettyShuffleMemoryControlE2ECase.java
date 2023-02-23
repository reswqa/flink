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

package org.apache.flink.streaming.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

/** E2E test for the netty shuffle memory control. */
class NettyShuffleMemoryControlE2ECase {
    static TestcontainersSettings testcontainersSettings =
            TestcontainersSettings.builder()
                    .logger(LoggerFactory.getLogger(NettyShuffleMemoryControlE2ECase.class))
                    .build();

    @RegisterExtension
    static FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(getFlinkContainersSettings())
                    .withTestcontainersSettings(testcontainersSettings)
                    .build();

    @Test
    void testNettyShuffleMemoryControl() {}

    private static FlinkContainersSettings getFlinkContainersSettings() {
        return FlinkContainersSettings.builder()
                .numTaskManagers(4)
                .numSlotsPerTaskManager(20)
                .fullConfiguration(getConfiguration())
                .build();
    }

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.ofMebiBytes(512));
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(128));
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(128));

        // Limits the direct memory to be one chunk (4M) plus some margins.
        configuration.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(7));

        // set only one arena per TM for boosting the netty internal memory overhead.
        configuration.set(NettyShuffleEnvironmentOptions.NUM_ARENAS, 1);

        return configuration;
    }
}
