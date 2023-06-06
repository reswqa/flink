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

package org.apache.flink.api.common;

import com.esotericsoftware.kryo.Serializer;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public interface SerializerContext {

    /** Returns the registered default Kryo Serializers. */
    LinkedHashMap<Class<?>, SerializableSerializer<?>> getDefaultKryoSerializers();

    /** Returns the registered default Kryo Serializer classes. */
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses();

    /** Returns the registered Kryo types. */
    LinkedHashSet<Class<?>> getRegisteredKryoTypes();

    /** Returns the registered types with their Kryo Serializer classes. */
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getRegisteredTypesWithKryoSerializerClasses();

    /** Returns the registered types with Kryo Serializers. */
    LinkedHashMap<Class<?>, SerializableSerializer<?>> getRegisteredTypesWithKryoSerializers();

    /** Returns the registered POJO types. */
    LinkedHashSet<Class<?>> getRegisteredPojoTypes();

    boolean isForceKryoEnabled();

    /** Returns whether the Apache Avro is the default serializer for POJOs. */
    boolean isForceAvroEnabled();

    /**
     * Checks whether generic types are supported. Generic types are types that go through Kryo
     * during serialization.
     *
     * <p>Generic types are enabled by default.
     */
    boolean hasGenericTypesDisabled();
}
