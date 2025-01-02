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
import org.apache.flink.datastream.api.extension.join.JoinExtension;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;

/**
 * This is an example of an e-commerce scenario used to calculate successful recommended orders. If a product
 * is recommended to a user and the user completes the purchase within five minutes, we call this
 * order as {@link RecommendationOrder}. This example expects to count the {@link
 * RecommendationOrder}s every ten minutes for the past hour.
 *
 * <p>The application involves two series of input data: the first is the {@link
 * RecommendationEvent} stream, which represents the recommendation event for a product, including
 * the product ID, user ID, and recommendation time. The second is the {@link OrderEvent} stream,
 * which captures the order events, including order ID, product ID, user ID and order time.
 *
 * <p>In order to analyse in ten minute intervals for the last hour, we use the ProcessingTime
 * sliding window, with a window size of one hour and a sliding step of ten minutes.
 *
 * <p>In order to calculate whether a product order is a RecommendedOrder, we join the {@link
 * RecommendationEvent} stream to the {@link OrderEvent} stream based on the product ID and user ID,
 * and judge by the time difference between the two.
 */
class RecommendationOrderExample implements Serializable {

    private static final int GOODS_NUMBER = 100;
    private static final int USER_NUMBER = 100;

    /**
     * A recommendation event represents a recommendation record that recommends a product to a
     * user.
     */
    public static class RecommendationEvent {
        private long productId;
        private long userId;
        private long timestamp;

        public RecommendationEvent(long productId, long userId, long timestamp) {
            this.productId = productId;
            this.userId = userId;
            this.timestamp = timestamp;
        }

        public long getProductId() {
            return productId;
        }

        public long getUserId() {
            return userId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /** A product purchase record indicating that a user purchased a product at a certain time. */
    public static class OrderEvent {
        private long orderId;
        private long productId;
        private long userId;
        private long timestamp;

        public OrderEvent(long orderId, long productId, long userId, long timestamp) {
            this.orderId = orderId;
            this.productId = productId;
            this.userId = userId;
            this.timestamp = timestamp;
        }

        public long getOrderId() {
            return orderId;
        }

        public long getProductId() {
            return productId;
        }

        public long getUserId() {
            return userId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * If a product is recommended to a user and the user completes the purchase within five
     * minutes, we call this order as {@link RecommendationOrder}.
     */
    public static class RecommendationOrder {
        private long orderId;
        private long productId;
        private long userId;
        private long recommendationTime;
        private long orderTime;

        public RecommendationOrder(
                long orderId,
                long productId,
                long userId,
                long recommendationTime,
                long orderTime) {
            this.orderId = orderId;
            this.productId = productId;
            this.userId = userId;
            this.recommendationTime = recommendationTime;
            this.orderTime = orderTime;
        }

        @Override
        public String toString() {
            return "RecommendationOrder{"
                    + "orderId="
                    + orderId
                    + ", productId="
                    + productId
                    + ", userId="
                    + userId
                    + ", recommendationTime="
                    + recommendationTime
                    + ", orderTime="
                    + orderTime
                    + '}';
        }
    }

    public static class ProductIdAndUserId implements Serializable, Comparable<ProductIdAndUserId> {
        private long productId;
        private long userId;

        public ProductIdAndUserId(long productId, long userId) {
            this.productId = productId;
            this.userId = userId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProductIdAndUserId that = (ProductIdAndUserId) o;
            return productId == that.productId && userId == that.userId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(productId, userId);
        }

        @Override
        public int compareTo(@NotNull ProductIdAndUserId o) {
            if (productId != o.productId) {
                return Long.compare(productId, o.productId);
            }
            return Long.compare(userId, o.userId);
        }
    }

    public static class RecommendationEventGenerateFunction
            implements GeneratorFunction<Long, RecommendationEvent> {

        private Random random = new Random();

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            random.setSeed(System.currentTimeMillis());
        }

        @Override
        public RecommendationEvent map(Long value) throws Exception {
            return new RecommendationEvent(
                    random.nextInt(GOODS_NUMBER),
                    random.nextInt(USER_NUMBER),
                    System.currentTimeMillis());
        }
    }

    public static class PurchaseEventGenerateFunction
            implements GeneratorFunction<Long, OrderEvent> {

        private Random random = new Random();

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            random.setSeed(System.currentTimeMillis());
        }

        @Override
        public OrderEvent map(Long value) throws Exception {
            return new OrderEvent(
                    random.nextInt(Integer.MAX_VALUE),
                    random.nextInt(GOODS_NUMBER),
                    random.nextInt(USER_NUMBER),
                    System.currentTimeMillis());
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // create recommendation event stream source and key by productId and userId
        KeyedPartitionStream<ProductIdAndUserId, RecommendationEvent> recommendationStream =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new DataGeneratorSource<RecommendationEvent>(
                                                new RecommendationEventGenerateFunction(),
                                                Long.MAX_VALUE,
                                                TypeInformation.of(RecommendationEvent.class))),
                                "RecommendationSource")
                        .withParallelism(1)
                        .keyBy(
                                event ->
                                        new ProductIdAndUserId(
                                                event.getProductId(), event.getUserId()));

        // create order event stream source and key by productId and userId
        KeyedPartitionStream<ProductIdAndUserId, OrderEvent> orderStream =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new DataGeneratorSource<OrderEvent>(
                                                new PurchaseEventGenerateFunction(),
                                                Long.MAX_VALUE,
                                                TypeInformation.of(OrderEvent.class))),
                                "OrderSource")
                        .withParallelism(1)
                        .keyBy(
                                event ->
                                        new ProductIdAndUserId(
                                                event.getProductId(), event.getUserId()));

        NonKeyedPartitionStream<RecommendationOrder> recommendationOrderStream =
                recommendationStream.connectAndProcess(
                        orderStream,
                        WindowExtension.apply(
                                // build sliding processing time window of 1 hour with 10 minute
                                // slide
                                WindowExtension.TimeWindows.ofSliding(
                                        Duration.ofHours(10),
                                        Duration.ofMinutes(2),
                                        WindowExtension.TimeWindows.TimeType.PROCESSING),
                                JoinExtension.wrapAsWindowProcessFunction(
                                        new JoinFunction<
                                                RecommendationEvent,
                                                OrderEvent,
                                                RecommendationOrder>() {

                                            @Override
                                            public void processRecord(
                                                    RecommendationEvent recommendationEvent,
                                                    OrderEvent orderEvent,
                                                    Collector<RecommendationOrder> output,
                                                    RuntimeContext ctx)
                                                    throws Exception {
                                                if (orderEvent.getTimestamp()
                                                                - recommendationEvent.getTimestamp()
                                                        < Duration.ofMinutes(5).toMillis()) {
                                                    // judge whether the order is a {@link
                                                    // RecommendationOrder}
                                                    output.collect(
                                                            new RecommendationOrder(
                                                                    orderEvent.getOrderId(),
                                                                    orderEvent.getProductId(),
                                                                    orderEvent.getUserId(),
                                                                    recommendationEvent
                                                                            .getTimestamp(),
                                                                    orderEvent.getTimestamp()));
                                                }
                                            }
                                        },
                                        JoinExtension.JoinType.INNER)));

        // print result
        recommendationOrderStream.toSink(new WrappedSink<>(new PrintSink<>()));

        env.execute("RecommendationOrderExample");
    }
}
