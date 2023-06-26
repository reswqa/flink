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

package org.apache.flink.processfunction.state;

import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.processfunction.api.state.StateDeclaration;

import java.util.Objects;

public class MapStateDeclarationImpl<K, V> extends AbstractStateDeclaration
        implements StateDeclaration.MapStateDeclaration {
    private final TypeDescriptor<K> keyTypeDescriptor;

    private final TypeDescriptor<V> valueTypeDescriptor;

    public MapStateDeclarationImpl(
            String name,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor,
            RedistributionMode redistributionMode) {
        super(name, redistributionMode);
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
    }

    public TypeDescriptor<K> getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    public TypeDescriptor<V> getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MapStateDeclarationImpl<?, ?> that = (MapStateDeclarationImpl<?, ?>) o;
        return Objects.equals(getKeyTypeDescriptor(), that.getKeyTypeDescriptor())
                && Objects.equals(getValueTypeDescriptor(), that.getValueTypeDescriptor());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getKeyTypeDescriptor(), getValueTypeDescriptor());
    }
}
