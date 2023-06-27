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

package org.apache.flink.configuration;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS;
import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL;
import static org.apache.flink.util.Preconditions.checkArgument;

public class ConfigOptionsUtils {

    public static Time getStandaloneClusterStartupPeriodTime(Configuration configuration) {
        final Time timeout;
        long standaloneClusterStartupPeriodTime =
                configuration.getLong(
                        ResourceManagerOptions.STANDALONE_CLUSTER_STARTUP_PERIOD_TIME);
        if (standaloneClusterStartupPeriodTime >= 0) {
            timeout = Time.milliseconds(standaloneClusterStartupPeriodTime);
        } else {
            timeout =
                    Time.milliseconds(
                            configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));
        }
        return timeout;
    }

    /**
     * @return extracted {@link MetricOptions#SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL} or {@code
     *     Optional.empty()} if {@link MetricOptions#SYSTEM_RESOURCE_METRICS} are disabled.
     */
    public static Optional<Time> getSystemResourceMetricsProbingInterval(
            Configuration configuration) {
        if (!configuration.getBoolean(SYSTEM_RESOURCE_METRICS)) {
            return Optional.empty();
        } else {
            return Optional.of(
                    Time.milliseconds(
                            configuration.getLong(SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL)));
        }
    }

    /**
     * Extracts the local state directories as defined by {@link
     * CheckpointingOptions#LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS}.
     *
     * @param configuration configuration object
     * @return array of configured directories (in order)
     */
    @Nonnull
    public static String[] parseLocalStateDirectories(Configuration configuration) {
        String configValue =
                configuration.getString(
                        CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, "");
        return ConfigurationUtils.splitPaths(configValue);
    }

    /**
     * Extracts the task manager directories for temporary files as defined by {@link
     * CoreOptions#TMP_DIRS}.
     *
     * @param configuration configuration object
     * @return array of configured directories (in order)
     */
    @Nonnull
    public static String[] parseTempDirectories(Configuration configuration) {
        return ConfigurationUtils.splitPaths(configuration.getString(CoreOptions.TMP_DIRS));
    }

    /**
     * Picks a temporary directory randomly from the given configuration.
     *
     * @param configuration to extract the temp directory from
     * @return a randomly picked temporary directory
     */
    @Nonnull
    public static File getRandomTempDirectory(Configuration configuration) {
        final String[] tmpDirectories = parseTempDirectories(configuration);

        Preconditions.checkState(
                tmpDirectories.length > 0,
                String.format(
                        "No temporary directory has been specified for %s",
                        CoreOptions.TMP_DIRS.key()));

        final int randomIndex = ThreadLocalRandom.current().nextInt(tmpDirectories.length);

        return new File(tmpDirectories[randomIndex]);
    }

    @VisibleForTesting
    public static Map<String, String> parseTmResourceDynamicConfigs(String dynamicConfigsStr) {
        Map<String, String> configs = new HashMap<>();
        String[] configStrs = dynamicConfigsStr.split(" ");

        checkArgument(
                configStrs.length % 2 == 0,
                "Dynamic option string contained odd number of arguments: #arguments=%s, (%s)",
                configStrs.length,
                dynamicConfigsStr);
        for (int i = 0; i < configStrs.length; ++i) {
            String configStr = configStrs[i];
            if (i % 2 == 0) {
                checkArgument(configStr.equals("-D"));
            } else {
                String[] configKV = configStr.split("=");
                checkArgument(configKV.length == 2);
                configs.put(configKV[0], configKV[1]);
            }
        }

        checkConfigContains(configs, TaskManagerOptions.CPU_CORES.key());
        checkConfigContains(configs, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key());
        checkConfigContains(configs, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key());
        checkConfigContains(configs, TaskManagerOptions.TASK_HEAP_MEMORY.key());
        checkConfigContains(configs, TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key());
        checkConfigContains(configs, TaskManagerOptions.NETWORK_MEMORY_MAX.key());
        checkConfigContains(configs, TaskManagerOptions.NETWORK_MEMORY_MIN.key());
        checkConfigContains(configs, TaskManagerOptions.MANAGED_MEMORY_SIZE.key());
        checkConfigContains(configs, TaskManagerOptions.JVM_METASPACE.key());
        checkConfigContains(configs, TaskManagerOptions.JVM_OVERHEAD_MIN.key());
        checkConfigContains(configs, TaskManagerOptions.JVM_OVERHEAD_MAX.key());
        checkConfigContains(configs, TaskManagerOptions.NUM_TASK_SLOTS.key());

        return configs;
    }

    private static void checkConfigContains(Map<String, String> configs, String key) {
        checkArgument(
                configs.containsKey(key), "Key %s is missing present from dynamic configs.", key);
    }
}
