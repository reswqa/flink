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

package org.apache.flink.core.memory;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;

public class WrappedMemorySegment extends MemorySegment {
    private String stack;

    private String reason;

    public WrappedMemorySegment(@Nonnull ByteBuffer buffer, @Nullable Object owner) {
        super(buffer, owner);
    }

    public WrappedMemorySegment setThreadDump(String reason) {
        StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
        StringBuilder stackTraceStr = new StringBuilder();
        for (StackTraceElement e : stacks) {
            stackTraceStr.append("\tat ").append(e).append("\n");
        }
        this.stack = stackTraceStr.toString();
        this.reason = reason;
        return this;
    }

    public static WrappedMemorySegment toWrapped(MemorySegment memorySegment) {
        Preconditions.checkState(memorySegment instanceof WrappedMemorySegment);
        return ((WrappedMemorySegment) memorySegment);
    }
}
