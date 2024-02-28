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

import org.apache.flink.process.api.context.StateManager;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

/** The default implementation of {@link StateManager}. */
public class DefaultStateManager implements StateManager {
    /**
     * {@link #getCurrentKey()} will return this if this is not null, otherwise return the key from
     * {@link #currentKeySupplier}.
     */
    @Nullable private Object overwriteCurrentKey;

    private final Supplier<Optional<Object>> currentKeySupplier;

    public DefaultStateManager(Supplier<Optional<Object>> currentKeySupplier) {
        this.currentKeySupplier = currentKeySupplier;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> Optional<K> getCurrentKey() {
        try {
            if (overwriteCurrentKey != null) {
                return Optional.of((K) overwriteCurrentKey);
            }
            return (Optional<K>) currentKeySupplier.get();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public void setCurrentKey(Object key) {
        overwriteCurrentKey = Preconditions.checkNotNull(key);
    }

    public void resetCurrentKey() {
        overwriteCurrentKey = null;
    }
}
