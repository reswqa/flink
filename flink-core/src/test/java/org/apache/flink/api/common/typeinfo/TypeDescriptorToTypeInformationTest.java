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
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TypeDescriptor} convert to {@link TypeInformation}. */
class TypeDescriptorToTypeInformationTest {
    @Test
    void testBasicDescriptor() {
        assertType(TypeDescriptors.LONG, Types.LONG);
        assertType(TypeDescriptors.STRING, Types.STRING);
    }

    @Test
    void testTupleDescriptor() {
        TupleTypeDescriptor<Tuple> tupleType =
                TypeDescriptors.TUPLE(TypeDescriptors.LONG, TypeDescriptors.STRING);
        assertType(tupleType, Types.TUPLE(Types.LONG, Types.STRING));
    }

    @Test
    void testPojoDescriptor() {
        assertType(TypeDescriptors.POJO(TestPojo.class), Types.POJO(TestPojo.class));

        Map<String, TypeDescriptor<?>> typeDescriptorMap = new HashMap<>();
        typeDescriptorMap.put("a", TypeDescriptors.LONG);
        typeDescriptorMap.put(
                "c", TypeDescriptors.TUPLE(TypeDescriptors.STRING, TypeDescriptors.LONG));
        TypeDescriptor<TestPojo> pojoType = TypeDescriptors.POJO(TestPojo.class, typeDescriptorMap);
        assertType(pojoType, Types.POJO(TestPojo.class));
    }

    private static <T> void assertType(
            TypeDescriptor<T> typeDescriptors, TypeInformation<T> typeInformation) {
        assertThat(TypeInformationUtils.fromTypeDescriptor(typeDescriptors))
                .isEqualTo(typeInformation);
    }

    public static class TestPojo {
        public long a;

        public Tuple2<String, Long> c;

        public TestPojo() {}

        public TestPojo(int a, Tuple2<String, Long> c) {
            this.a = a;
            this.c = c;
        }
    }
}
