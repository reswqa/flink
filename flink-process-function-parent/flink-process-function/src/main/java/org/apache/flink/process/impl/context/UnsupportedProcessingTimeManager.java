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

package org.apache.flink.process.impl.context;

import org.apache.flink.process.api.context.ProcessingTimeManager;

/**
 * The special implementation of {@link ProcessingTimeManager}, all its methods are not supported.
 * This is used for context that can not define the key.
 */
public class UnsupportedProcessingTimeManager implements ProcessingTimeManager {
    public static UnsupportedProcessingTimeManager INSTANCE =
            new UnsupportedProcessingTimeManager();

    private UnsupportedProcessingTimeManager() {}

    @Override
    public void registerProcessingTimer(long timestamp) {
        throw new UnsupportedOperationException(
                "Register processing timer is unsupported in current context.");
    }

    @Override
    public void deleteProcessingTimeTimer(long timestamp) {
        throw new UnsupportedOperationException(
                "Delete processing timer is unsupported in current context.");
    }

    @Override
    public long currentProcessingTime() {
        throw new UnsupportedOperationException(
                "Get current processing time is unsupported in current context.");
    }
}
