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

package org.apache.flink.processfunction.api.builtin;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.function.ConsumerFunction;

public class Sinks {
    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE = Class.forName("org.apache.flink.processfunction.builtin.SinksImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Please ensure that flink-process-function in your class path");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Sink<T> consumer(ConsumerFunction<T> consumer) {
        try {
            return (Sink<T>)
                    INSTANCE.getMethod("consumer", ConsumerFunction.class).invoke(null, consumer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
