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

package org.apache.flink.api.common.eventtime;

import java.io.Serializable;

public interface ProcessWatermark<T extends ProcessWatermark<T>>
        extends Comparable<T>, Serializable {
    // The definition of this interface may require further consideration.
    // This interface extends from Comparable, so that we can compare two watermark in some
    // value-based alignment algorithm. But in the future, it may be necessary for us to provide
    // some alignment algorithms that are not based on comparison. In such cases, compareTo needs to
    // return 0 always.
}
