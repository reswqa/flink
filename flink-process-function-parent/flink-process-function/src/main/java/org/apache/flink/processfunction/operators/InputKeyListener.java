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

package org.apache.flink.processfunction.operators;

import org.apache.flink.processfunction.DefaultRuntimeContext;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.util.function.TriConsumer;

import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

interface InputKeyListener {
    void keySelected(Object newKey);

    void endOfInput();

    class SortedInputKeyListener<OUT> implements InputKeyListener {
        private Object oldKey;

        private final BiConsumer<Consumer<OUT>, RuntimeContext> endOfPartitionNotifier;

        private final Consumer<OUT> output;

        private final DefaultRuntimeContext ctx;

        public SortedInputKeyListener(
                Consumer<OUT> output,
                DefaultRuntimeContext ctx,
                BiConsumer<Consumer<OUT>, RuntimeContext> endOfPartitionNotifier) {
            this.endOfPartitionNotifier = endOfPartitionNotifier;
            this.output = output;
            this.ctx = ctx;
        }

        @Override
        public void keySelected(Object newKey) {
            if (newKey == null) {
                return;
            }

            if (!newKey.equals(oldKey) && oldKey != null) {
                ctx.setCurrentKey(oldKey);
                endOfPartitionNotifier.accept(output, ctx);
                // reset endOfPartitionKey
                ctx.resetCurrentKey();
            }
            oldKey = newKey;
        }

        @Override
        public void endOfInput() {
            ctx.setCurrentKey(oldKey);
            endOfPartitionNotifier.accept(output, ctx);
            ctx.resetCurrentKey();
        }
    }

    class UnSortedInputKeyListener<OUT> implements InputKeyListener {
        private final BiConsumer<Consumer<OUT>, RuntimeContext> endOfPartitionNotifier;

        private final Consumer<OUT> output;

        private final DefaultRuntimeContext ctx;

        /** Used to store all the keys seen by this input. */
        private final HashSet<Object> allKeys = new HashSet<>();

        public UnSortedInputKeyListener(
                Consumer<OUT> output,
                DefaultRuntimeContext ctx,
                BiConsumer<Consumer<OUT>, RuntimeContext> endOfPartitionNotifier) {
            this.output = output;
            this.ctx = ctx;
            this.endOfPartitionNotifier = endOfPartitionNotifier;
        }

        @Override
        public void keySelected(Object newKey) {
            allKeys.add(newKey);
        }

        @Override
        public void endOfInput() {
            for (Object key : allKeys) {
                ctx.setCurrentKey(key);
                endOfPartitionNotifier.accept(output, ctx);
            }
            // reset current key
            ctx.resetCurrentKey();
        }
    }

    // TODO Consider extract common part with Sorted / UnSorted InputKeyListener.
    class SortedTwoOutputInputKeyListener<OUT1, OUT2> implements InputKeyListener {
        private Object oldKey;

        private final TriConsumer<Consumer<OUT1>, Consumer<OUT2>, RuntimeContext>
                endOfPartitionNotifier;

        private final Consumer<OUT1> output1;

        private final Consumer<OUT2> output2;

        private final DefaultRuntimeContext ctx;

        public SortedTwoOutputInputKeyListener(
                TriConsumer<Consumer<OUT1>, Consumer<OUT2>, RuntimeContext> endOfPartitionNotifier,
                Consumer<OUT1> output1,
                Consumer<OUT2> output2,
                DefaultRuntimeContext ctx) {
            this.endOfPartitionNotifier = endOfPartitionNotifier;
            this.output1 = output1;
            this.output2 = output2;
            this.ctx = ctx;
        }

        @Override
        public void keySelected(Object newKey) {
            if (newKey == null) {
                return;
            }

            if (!newKey.equals(oldKey) && oldKey != null) {
                ctx.setCurrentKey(oldKey);
                endOfPartitionNotifier.accept(output1, output2, ctx);
                // reset endOfPartitionKey
                ctx.resetCurrentKey();
            }
            oldKey = newKey;
        }

        @Override
        public void endOfInput() {
            ctx.setCurrentKey(oldKey);
            endOfPartitionNotifier.accept(output1, output2, ctx);
            ctx.resetCurrentKey();
        }
    }

    class UnsortedTwoOutputInputKeyListener<OUT1, OUT2> implements InputKeyListener {
        private final TriConsumer<Consumer<OUT1>, Consumer<OUT2>, RuntimeContext>
                endOfPartitionNotifier;

        private final Consumer<OUT1> output1;

        private final Consumer<OUT2> output2;

        private final DefaultRuntimeContext ctx;

        /** Used to store all the keys seen by this input. */
        private final HashSet<Object> allKeys = new HashSet<>();

        public UnsortedTwoOutputInputKeyListener(
                TriConsumer<Consumer<OUT1>, Consumer<OUT2>, RuntimeContext> endOfPartitionNotifier,
                Consumer<OUT1> output1,
                Consumer<OUT2> output2,
                DefaultRuntimeContext ctx) {
            this.endOfPartitionNotifier = endOfPartitionNotifier;
            this.output1 = output1;
            this.output2 = output2;
            this.ctx = ctx;
        }

        @Override
        public void keySelected(Object newKey) {
            allKeys.add(newKey);
        }

        @Override
        public void endOfInput() {
            for (Object key : allKeys) {
                ctx.setCurrentKey(key);
                endOfPartitionNotifier.accept(output1, output2, ctx);
            }
            // reset current key
            ctx.resetCurrentKey();
        }
    }
}
