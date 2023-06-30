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

package org.apache.flink.processfunction.connector;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.function.ConsumerFunction;

import java.io.IOException;

/** SinkV2 implementation for ConsumerSink. */
public class ConsumerSink<T> implements Sink<T> {
    private final ConsumerFunction<T> consumerFunction;

    public ConsumerSink(ConsumerFunction<T> consumerFunction) {
        this.consumerFunction = consumerFunction;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new ConsumerSinkWriter();
    }

    private class ConsumerSinkWriter implements SinkWriter<T> {
        private boolean isClosed = false;

        @Override
        public void write(T element, Context context) throws IOException, InterruptedException {
            if (!isClosed) {
                consumerFunction.accept(element);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // do nothing.
        }

        @Override
        public void close() throws Exception {
            isClosed = true;
        }
    }
}
