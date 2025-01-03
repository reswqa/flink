package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.extension.window.WindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.window.TimeWindow;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class StatisticPopularProductInEachShopExample {
    private static final int RECORD_NUMBER = 100000;
    private static final int SHOP_NUMBER = 100;

    /** The {@link EnrichOrderExample.Order} contains essential order information. */
    public static class Order {
        private long orderId;
        private long productId;
        private long shopId;
        private long userId;
        private long orderTime;

        public Order(long orderId, long productId, long shopId, long userId, long orderTime) {
            this.orderId = orderId;
            this.productId = productId;
            this.shopId = shopId;
            this.userId = userId;
            this.orderTime = orderTime;
        }

        public long getOrderId() {
            return orderId;
        }

        public long getProductId() {
            return productId;
        }

        public long getShopId() {
            return shopId;
        }

        public long getUserId() {
            return userId;
        }

        public long getOrderTime() {
            return orderTime;
        }
    }

    public static class PopularProductInEachShop {
        private long shopId;
        private long productId;
        private long numberOfSales;

        public PopularProductInEachShop(long shopId, long productId, long numberOfSales) {
            this.shopId = shopId;
            this.productId = productId;
            this.numberOfSales = numberOfSales;
        }

        @Override
        public String toString() {
            return "PopularProductInEachShop{" +
                    "shopId=" + shopId +
                    ", productId=" + productId +
                    ", numberOfSales=" + numberOfSales +
                    '}';
        }
    }

    public static class OrderGenerateFunction
            implements GeneratorFunction<Long, Order> {

        private Random random = new Random();

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            random.setSeed(System.currentTimeMillis());
        }

        @Override
        public Order map(Long value) throws Exception {
            return new Order(
                    random.nextInt(), random.nextInt(), random.nextInt(SHOP_NUMBER), random.nextInt(), System.currentTimeMillis());
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // create recommendation event stream source and key by productId and userId
        NonKeyedPartitionStream<Order> orderStream =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new DataGeneratorSource<Order>(
                                                new OrderGenerateFunction(),
                                                Long.MAX_VALUE,
                                                TypeInformation.of(Order.class))),
                                "OrderSource")
                        .withParallelism(1);;

        orderStream
                .keyBy(Order::getShopId)
                .process(
                        WindowExtension.apply(
                                WindowExtension.TimeWindows.ofTumbling(
                                        Duration.ofHours(1),
                                        WindowExtension.TimeWindows.TimeType.EVENT),
                                (WindowProcessFunction<Iterable<Order>, PopularProductInEachShop, TimeWindow>) (orders, output, ctx, windowContext) -> {
                                    long shopId = -1;
                                    Map<Long, Long> productSalesMap = new HashMap();

                                    for (Order order : orders) {
                                        productSalesMap.compute(
                                                order.getProductId(),
                                                (k, v) -> v == null ? 1 : v + 1);
                                    }
                                    Order order = orders.iterator().next();

//                                    new PopularProductInEachShop()
                                }
                        )
                );

    }
}
