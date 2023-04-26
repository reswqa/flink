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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/** This is a wrapper for outputs to check whether its record output needs to be counted. */
@Internal
public interface OutputWithRecordsCountCheck<OUT> {
    /**
     * Collect a record and check if it needs to be counted.
     *
     * @param record The record to collect.
     */
    boolean collectAndCheckIfCountNeeded(StreamRecord<OUT> record);

    /**
     * Collect a record to the side output identified by the given {@link OutputTag} and check if it
     * needs to be counted.
     *
     * @param record The record to collect.
     * @param outputTag Identification of side outputs.
     */
    <X> boolean collectAndCheckIfCountNeeded(OutputTag<X> outputTag, StreamRecord<X> record);
}
