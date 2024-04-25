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

package org.apache.flink.table.connector.source.lookup.cache;

import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.partitioner.RowDataCustomPartitioner;

/** Missing doc */
public interface PartitionedLookupProvider extends LookupRuntimeProvider {
    /**
     * Build a {@link PartitionedLookupProvider} from the specified {@link LookupRuntimeProvider}.
     */
    static PartitionedLookupProvider of(
            RowDataCustomPartitioner partitioner, LookupRuntimeProvider provider) {
        return new PartitionedLookupProvider() {
            @Override
            public RowDataCustomPartitioner getPartitioner() {
                return partitioner;
            }

            @Override
            public LookupRuntimeProvider getProvider() {
                return provider;
            }
        };
    }

    RowDataCustomPartitioner getPartitioner();

    LookupRuntimeProvider getProvider();
}
