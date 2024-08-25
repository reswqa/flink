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

package org.apache.flink.streaming.util.watermark;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.api.function.ProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils class for {@link Watermark}. */
public class WatermarkUtils {

    /**
     * Retrieve the user-defined {@link WatermarkDeclaration}s of {@link ProcessFunction}. The
     * {@link WatermarkDeclaration} defined by the source operator can be retrieved from {@link
     * SourceOperatorFactory#getSourceWatermarkDeclarations()}.
     */
    public static Collection<? extends WatermarkDeclaration> getWatermarkDeclarations(
            StreamOperator<?> streamOperator) {
        if (streamOperator instanceof AbstractUdfStreamOperator) {
            Function f = ((AbstractUdfStreamOperator<?, ?>) streamOperator).getUserFunction();
            if (f instanceof ProcessFunction) {
                return ((ProcessFunction) f).watermarkDeclarations();
            }
        }
        return Collections.emptySet();
    }

    /**
     * Convert user-oriented {@link WatermarkDeclaration} instance to internal-oriented {@link
     * AbstractInternalWatermarkDeclaration} instance.
     */
    public static Set<AbstractInternalWatermarkDeclaration<?>>
            convertToInternalWatermarkDeclarations(
                    Set<WatermarkDeclaration> watermarkDeclarations) {
        return watermarkDeclarations.stream()
                .map(AbstractInternalWatermarkDeclaration::from)
                .collect(Collectors.toSet());
    }
}
