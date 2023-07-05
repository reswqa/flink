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

package org.apache.flink.processfunction.builtin;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.processfunction.connector.FromCollectionSource;
import org.apache.flink.processfunction.connector.SupplierSource;
import org.apache.flink.util.function.SupplierFunction;

import java.util.Collection;

/** This class provides some built-in sources for convenience. */
public class SourcesImpl {

    public static <T> Source<T, ?, ?> supplier(SupplierFunction<T> supplier) {
        return new SupplierSource<>(supplier);
    }

    public static <T> Source<T, ?, ?> collection(Collection<T> collection) {
        return new FromCollectionSource<>(collection);
    }
}
