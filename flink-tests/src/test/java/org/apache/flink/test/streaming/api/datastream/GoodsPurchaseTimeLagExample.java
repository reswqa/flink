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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.extension.join.JoinExtension;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import java.io.Serializable;
import java.time.Duration;
import java.util.Random;

/**
 * This is an example from an e-commerce scenario, wherein every ten minutes compute the time
 * difference (called {@link GoodsPurchaseTimeLag}) between each goods being clicked and
 * subsequently ordered within the last hour.
 *
 * <p>The application involves two series of input data: the first is the {@link GoodsClickEvent}
 * stream, which tracks click events for goods and includes the goods ID and click timestamp. The
 * second is the {@link PurchaseEvent} stream, which captures order events and comprises the goods
 * ID and order timestamp.
 *
 * <p>To analyze the data in ten-minute intervals, we utilize an EventTime sliding window with a
 * one-hour window size and a sliding step of ten minutes.
 *
 * <p>To calculate the time difference between clicking on a goods and placing an order, we join the
 * {@link GoodsClickEvent} stream with the {@link PurchaseEvent} stream based on goods ID and
 * calculate the time difference accordingly.
 */
class GoodsPurchaseTimeLagExample implements Serializable {

    private static final int GOODS_NUMBER = 10000;

    public static class GoodsClickEvent {
        private long goodsId;
        private long clickTime;

        public GoodsClickEvent(long goodsId, long clickTime) {
            this.goodsId = goodsId;
            this.clickTime = clickTime;
        }

        public long getGoodsId() {
            return goodsId;
        }

        public long getClickTime() {
            return clickTime;
        }
    }

    public static class PurchaseEvent {
        private long goodsId;
        private long purchaseTime;

        public PurchaseEvent(long goodsId, long purchaseTime) {
            this.goodsId = goodsId;
            this.purchaseTime = purchaseTime;
        }

        public long getGoodsId() {
            return goodsId;
        }

        public long getPurchaseTime() {
            return purchaseTime;
        }
    }

    public static class GoodsPurchaseTimeLag {
        private long goodsId;
        private long timeLag;

        public GoodsPurchaseTimeLag(long goodsId, long timeLag) {
            this.goodsId = goodsId;
            this.timeLag = timeLag;
        }

        public long getGoodsId() {
            return goodsId;
        }

        public long getTimeLag() {
            return timeLag;
        }

        @Override
        public String toString() {
            return "GoodsPurchaseTimeLag{" + "goodsId=" + goodsId + ", timeLag=" + timeLag + '}';
        }
    }

    public static class GoodsClickEventGenerateFunction
            implements GeneratorFunction<Long, GoodsClickEvent> {

        private Random random = new Random();

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            random.setSeed(System.currentTimeMillis());
        }

        @Override
        public GoodsClickEvent map(Long value) throws Exception {
            return new GoodsClickEvent(random.nextInt(GOODS_NUMBER), System.currentTimeMillis());
        }
    }

    public static class PurchaseEventGenerateFunction
            implements GeneratorFunction<Long, PurchaseEvent> {

        private Random random = new Random();

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            random.setSeed(System.currentTimeMillis());
        }

        @Override
        public PurchaseEvent map(Long value) throws Exception {
            return new PurchaseEvent(random.nextInt(GOODS_NUMBER), System.currentTimeMillis());
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<GoodsClickEvent> clickStream =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new DataGeneratorSource<GoodsClickEvent>(
                                                new GoodsClickEventGenerateFunction(),
                                                Long.MAX_VALUE,
                                                TypeInformation.of(GoodsClickEvent.class))),
                                "ClickEventSource")
                        .withParallelism(1);

        NonKeyedPartitionStream<PurchaseEvent> purchaseStream =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new DataGeneratorSource<PurchaseEvent>(
                                                new PurchaseEventGenerateFunction(),
                                                Long.MAX_VALUE,
                                                TypeInformation.of(PurchaseEvent.class))),
                                "PurchaseEventSource")
                        .withParallelism(1);

        EventTimeWatermarkStrategy<GoodsClickEvent> clickEventWatermarkStrategy =
                EventTimeWatermarkStrategy.<GoodsClickEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(30L))
                        .withIdleness(Duration.ofMinutes(1L));
        KeyedPartitionStream<Long, GoodsClickEvent> keyedClickStream =
                clickStream
                        .process(
                                EventTimeExtension.extractEventTimeAndWatermark(
                                        GoodsClickEvent::getClickTime, clickEventWatermarkStrategy))
                        .keyBy(GoodsClickEvent::getGoodsId);

        EventTimeWatermarkStrategy<PurchaseEvent> purchaseEventWatermarkStrategy =
                EventTimeWatermarkStrategy.<PurchaseEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(30L))
                        .withIdleness(Duration.ofMinutes(1L));
        KeyedPartitionStream<Long, PurchaseEvent> keyedPurchaseStream =
                purchaseStream
                        .process(
                                EventTimeExtension.extractEventTimeAndWatermark(
                                        PurchaseEvent::getPurchaseTime,
                                        purchaseEventWatermarkStrategy))
                        .keyBy(PurchaseEvent::getGoodsId);

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<GoodsPurchaseTimeLag>
                timeLagStream =
                        keyedClickStream.connectAndProcess(
                                keyedPurchaseStream,
                                WindowExtension.apply(
                                        WindowExtension.TimeWindows.ofTwoInputSliding(
                                                Duration.ofSeconds(10),
                                                Duration.ofSeconds(2),
                                                WindowExtension.TimeWindows.TimeType.EVENT),
                                        JoinExtension.wrapAsWindowProcessFunction(
                                                new JoinFunction<
                                                        GoodsClickEvent,
                                                        PurchaseEvent,
                                                        GoodsPurchaseTimeLag>() {

                                                    @Override
                                                    public void processRecord(
                                                            GoodsClickEvent clickEvent,
                                                            PurchaseEvent purchaseEvent,
                                                            Collector<GoodsPurchaseTimeLag> output,
                                                            RuntimeContext ctx)
                                                            throws Exception {
                                                        output.collect(
                                                                new GoodsPurchaseTimeLag(
                                                                        purchaseEvent.getGoodsId(),
                                                                        purchaseEvent
                                                                                        .getPurchaseTime()
                                                                                - clickEvent
                                                                                        .getClickTime()));
                                                    }
                                                },
                                                JoinExtension.JoinType.INNER)));

        timeLagStream.toSink(new WrappedSink<>(new PrintSink<>()));

        env.execute("GoodsPurchaseTimeLagExample");
    }
}
