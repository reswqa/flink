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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils for {@link TypeInformation}. */
public class TypeInformationUtils {
    // ------------------------------------------------------------------------

    /**
     * Creates a TypeInformation for the type described by the given class.
     *
     * <p>This method only works for non-generic types. For generic types, use the {@link
     * #of(TypeHint)} method.
     *
     * @param typeClass The class of the type.
     * @param <T> The generic type.
     * @return The TypeInformation object for the type described by the hint.
     */
    public static <T> TypeInformation<T> of(Class<T> typeClass) {
        try {
            return TypeExtractor.createTypeInfo(typeClass);
        } catch (InvalidTypesException e) {
            throw new FlinkRuntimeException(
                    "Cannot extract TypeInformation from Class alone, because generic parameters are missing. "
                            + "Please use TypeInformationUtils.of(TypeHint) instead, or another equivalent method in the API that "
                            + "accepts a TypeHint instead of a Class. "
                            + "For example for a Tuple2<Long, String> pass a 'new TypeHint<Tuple2<Long, String>>(){}'.");
        }
    }

    /**
     * Creates a TypeInformation for a generic type via a utility "type hint". This method can be
     * used as follows:
     *
     * <pre>{@code
     * TypeInformation<Tuple2<String, Long>> info = TypeInformationUtils.of(new TypeHint<Tuple2<String, Long>>(){});
     * }</pre>
     *
     * @param typeHint The hint for the generic type.
     * @param <T> The generic type.
     * @return The TypeInformation object for the type described by the hint.
     */
    public static <T> TypeInformation<T> of(TypeHint<T> typeHint) {
        return typeHint.getTypeInfo();
    }

    public static TypeInformation<?> fromTypeDescriptor(TypeDescriptor<?> typeDescriptor) {
        if (typeDescriptor instanceof BasicTypeDescriptor) {
            return fromBasicTypeDescriptor((BasicTypeDescriptor<?>) typeDescriptor);
        } else if (typeDescriptor instanceof TupleTypeDescriptor) {
            TupleTypeDescriptor<?> tupleTypeDescriptor = (TupleTypeDescriptor<?>) typeDescriptor;
            return fromTupleDescriptor(tupleTypeDescriptor);
        } else if (typeDescriptor instanceof PojoDescriptor) {
            PojoDescriptor<?> pojoDescriptor = (PojoDescriptor<?>) typeDescriptor;
            return fromPojoDescriptor(pojoDescriptor);
        }
        throw new IllegalArgumentException(
                String.format("unsupported type descriptor %s.", typeDescriptor));
    }

    private static TupleTypeInfo<Tuple> fromTupleDescriptor(
            TupleTypeDescriptor tupleTypeDescriptor) {
        TypeDescriptor[] typeDescriptors = tupleTypeDescriptor.getTypeDescriptors();
        TypeInformation<?>[] typeInformations =
                Arrays.stream(typeDescriptors)
                        .map(TypeInformationUtils::fromTypeDescriptor)
                        .toArray(TypeInformation[]::new);
        return new TupleTypeInfo<>(typeInformations);
    }

    private static TypeInformation<?> fromPojoDescriptor(PojoDescriptor<?> pojoDescriptor) {
        Class<?> pojoClass = pojoDescriptor.getPojoClass();
        Map<String, TypeDescriptor<?>> fields = pojoDescriptor.getFields();
        if (fields == null) {
            // try extracts from class
            return Types.POJO(pojoClass);
        }
        Map<String, TypeInformation<?>> typeInformationMap =
                fields.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        (e) -> fromTypeDescriptor(e.getValue())));
        return Types.POJO(pojoClass, typeInformationMap);
    }

    private static TypeInformation<?> fromBasicTypeDescriptor(
            BasicTypeDescriptor<?> typeDescriptors) {
        if (String.class.equals(typeDescriptors.getTypeClass())) {
            return Types.STRING;
        } else if (Long.class.equals(typeDescriptors.getTypeClass())) {
            return Types.LONG;
        }
        throw new RuntimeException("unsupported basic class.");
    }
}
