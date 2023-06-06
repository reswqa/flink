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

package org.apache.flink.api.common.typeinfo;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

public class PojoDescriptor<T> implements TypeDescriptor<T> {
    private final Class<T> pojoClass;

    @Nullable private final Map<String, TypeDescriptor<?>> fields;

    public PojoDescriptor(Class<T> pojoClass, @Nullable Map<String, TypeDescriptor<?>> fields) {
        this.pojoClass = pojoClass;
        this.fields = fields;
    }

    public PojoDescriptor(Class<T> pojoClass) {
        this(pojoClass, null);
    }

    public Class<T> getPojoClass() {
        return pojoClass;
    }

    @Nullable
    public Map<String, TypeDescriptor<?>> getFields() {
        return fields;
    }

    @Override
    public Class<T> getTypeClass() {
        return pojoClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PojoDescriptor<?> that = (PojoDescriptor<?>) o;
        return Objects.equals(getPojoClass(), that.getPojoClass())
                && Objects.equals(getFields(), that.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPojoClass(), getFields());
    }
}
