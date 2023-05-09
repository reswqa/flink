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

package org.apache.flink.runtime.rest.handler.job;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.INPUT_EXCLUSIVE_BUFFERS_USAGE;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.INPUT_FLOATING_BUFFERS_USAGE;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.INPUT_POOL_USAGE;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.INPUT_QUEUE_LENGTH;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.INPUT_QUEUE_SIZE;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.OUTPUT_POOL_USAGE;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.OUTPUT_QUEUE_LENGTH;
import static org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler.OUTPUT_QUEUE_SIZE;

/** Tests for {@link JobVertexNetworkInfoHandler}. */
class JobVertexNetworkInfoHandlerTest {
    @Test
    void testA() {
        System.out.println(OUTPUT_QUEUE_SIZE);
        System.out.println(OUTPUT_QUEUE_LENGTH);
        System.out.println(OUTPUT_POOL_USAGE);
        System.out.println(INPUT_QUEUE_SIZE);
        System.out.println(INPUT_QUEUE_LENGTH);
        System.out.println(INPUT_POOL_USAGE);
        System.out.println(INPUT_FLOATING_BUFFERS_USAGE);
        System.out.println(INPUT_EXCLUSIVE_BUFFERS_USAGE);
    }
}
