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

import org.apache.flink.api.java.tuple.Tuple;

import java.util.Map;

public class TypeDescriptors {
    public static BasicTypeDescriptor<String> STRING = new BasicTypeDescriptor<>(String.class);

    public static BasicTypeDescriptor<Long> LONG = new BasicTypeDescriptor<>(Long.class);

    public static <T extends Tuple> TupleTypeDescriptor<T> TUPLE(
            TypeDescriptor<?>... typeDescriptors) {
        return new TupleTypeDescriptor<>(typeDescriptors);
    }

    public static <T> PojoDescriptor<T> POJO(
            Class<T> pojoClass, Map<String, TypeDescriptor<?>> fields) {
        return new PojoDescriptor<>(pojoClass, fields);
    }

    public static <T> PojoDescriptor<T> POJO(Class<T> pojoClass) {
        return new PojoDescriptor<>(pojoClass);
    }
}
