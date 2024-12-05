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
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.timer.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * This example illustrates how to count the number of clicks on each news at 1 minute, 5 minutes,
 * 10 minutes, and 1 hour after publication.
 *
 * <p>The input consists of a series of {@link NewsEvent}s, which fall into two categories: news
 * releases and news clicks. Each {@link NewsEvent} contains three components: the event type, the
 * news ID and the timestamp. Notably, there is only one event of type {@link NewsEventType#RELEASE}
 * for each news.
 *
 * <p>Regarding the statistics of news click counts:
 *
 * <ul>
 *   <li>When the news release event arrives, we will set up timers for that news at release time +
 *       1 minute, release time + 5 minutes, release time + 10 minutes, release time + 30 minutes,
 *       and release time + 1 hour.
 *   <li>When the news click event arrives, we will record all click times for each news and save
 *       them as a list.
 *   <li>When the timers are triggered, we will count the number of clicks within the specified
 *       duration since the news was released.
 *   <li>For example, if news is released at 10:00:00, we will set timers for 10:01:00, 10:05:00,
 *       10:10:00, 10:30:00, and 11:00:00. When the timer for 10:01:00 is triggered, we will count
 *       and output the number of click events within the time interval of 10:00:00 to 10:01:00.
 * </ul>
 */
public class StatisticNewsClickNumberExample {

    // number of generated news
    private static final int NEWS_NUMBER = 10000;

    // we will count the number of clicks within 1min/5min/10min/30min/1hour after the news release
    //    private static final Duration[] TIMES_AFTER_NEWS_RELEASE =
    //            new Duration[] {
    //                Duration.ofMinutes(1),
    //                Duration.ofMinutes(5),
    //                Duration.ofMinutes(10),
    //                Duration.ofMinutes(30),
    //                Duration.ofHours(1)
    //            };

    private static final Duration[] TIMES_AFTER_NEWS_RELEASE =
            new Duration[] {
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                Duration.ofSeconds(5),
                Duration.ofSeconds(10)
            };

    /**
     * The type of {@link NewsEvent}, note that only one event of type {@link NewsEventType#RELEASE}
     * for each news.
     */
    public enum NewsEventType {
        RELEASE,
        CLICK
    }

    /**
     * The {@link NewsEvent} represents a event on news, containing the event type, news id and the
     * timestamp.
     */
    public static class NewsEvent {
        private NewsEventType type;
        private long newsId;
        private long timestamp;

        public NewsEvent(NewsEventType type, long newsId, long timestamp) {
            this.type = type;
            this.newsId = newsId;
            this.timestamp = timestamp;
        }

        public NewsEventType getType() {
            return type;
        }

        public long getNewsId() {
            return newsId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * The {@link NewsClickNumber} represents the number of clicks on news within a specified
     * duration following its release. For example, NewsClickNumber{newsId="12345678",
     * timeAfterRelease=60000, clickNumber=132} indicates that the news "12345678" has been clicked
     * 132 times within 60,000 milliseconds after its release.
     */
    public static class NewsClickNumber {
        private long newsId;
        private long timeAfterRelease;
        private long clickNumber;

        public NewsClickNumber(long newsId, long timeAfterRelease, long clickNumber) {
            this.newsId = newsId;
            this.timeAfterRelease = timeAfterRelease;
            this.clickNumber = clickNumber;
        }

        public long getNewsId() {
            return newsId;
        }

        public long getTimeAfterRelease() {
            return timeAfterRelease;
        }

        public long getClickNumber() {
            return clickNumber;
        }

        @Override
        public String toString() {
            return "NewsClickNumber{"
                    + "newsId="
                    + newsId
                    + ", timeAfterRelease="
                    + timeAfterRelease
                    + ", clickNumber="
                    + clickNumber
                    + '}';
        }
    }

    public static class NewsClickEventGenerateFunction
            implements GeneratorFunction<Long, NewsEvent> {

        private Random random = new Random();
        private Set<Integer> newsIdSet = new HashSet<>();

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            random.setSeed(System.currentTimeMillis());
        }

        @Override
        public NewsEvent map(Long value) throws Exception {
            long time = System.currentTimeMillis();
            int newsId = random.nextInt(NEWS_NUMBER);
            if (newsIdSet.contains(newsId)) {
                return new NewsEvent(NewsEventType.CLICK, newsId, time);
            } else {
                newsIdSet.add(newsId);
                return new NewsEvent(NewsEventType.RELEASE, newsId, time);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // the input consists of a series of {code NewsEvent}s, which include two types: news
        // release event and news click event.
        NonKeyedPartitionStream<NewsEvent> source = createSource(env);

        // extract event time and generate the event time watermark
        NonKeyedPartitionStream<NewsClickNumber> clickNumberStream =
                source.process(
                                // the timestamp field of the input is considered to be the
                                // event time
                                EventTimeExtension.newEventTimeWatermarkGeneratorBuilder(
                                                NewsEvent::getTimestamp)
                                        // generate event time watermarks every 200ms
                                        .periodicWatermark(Duration.ofMillis(200))
                                        // if the input is idle for more than 30 seconds, it
                                        // is ignored during the event time watermark
                                        // combination process
                                        .withIdleness(Duration.ofSeconds(30))
                                        // set the maximum out-of-order time of the event to
                                        // 30 seconds, meaning that if an event is received
                                        // at 12:00:00, then no further events should be
                                        // received earlier than 11:59:30
                                        .withMaxOutOfOrderTime(Duration.ofSeconds(10))
                                        // build the event time watermark generator as
                                        // ProcessFunction
                                        .buildAsProcessFunction())
                        // key by the news id
                        .keyBy(NewsEvent::getNewsId)
                        // count the click number of each news
                        .process(
                                EventTimeExtension.wrapAsEventTimeProcessFunction(
                                        new CountNewsClickNumberProcessFunction()));

        // print the number of clicks within 1 minute, 5 minutes, 10 minutes, and 1 hour after news
        // release
        clickNumberStream.toSink(new WrappedSink<>(new PrintSink<>()));

        env.execute("StatisticNewsClickNumberExample");
    }

    /**
     * This process function will consume {@link NewsEvent} and count the number of clicks within 1
     * minute, 5 minutes, 10 minutes, 30 minutes and 1 hour of the news releasing and send the
     * results {@link NewsClickNumber} to the output.
     *
     * <p>To achieve the goal, we will register a series of timers for the news, which will be
     * triggered at the time of the news's release time + 1 minute/5 minutes/10 minutes/30 minutes/1
     * hour, and record a list of the click times of each news. In the timer callback {@code
     * onEventTimer}, we will count the number of clicks between the news release time and the timer
     * trigger timer and send the result to the output.
     */
    public static class CountNewsClickNumberProcessFunction
            implements OneInputEventTimeStreamProcessFunction<NewsEvent, NewsClickNumber> {

        private EventTimeManager eventTimeManager;

        // news id to release time
        private final Map<Long, Long> releaseTimeOfNews = new HashMap<>();

        // news id to click time list
        private final Map<Long, List<Long>> clickTimeListOfNews = new HashMap<>();

        @Override
        public void initEventTimeExtension(EventTimeManager eventTimeManager) {
            this.eventTimeManager = eventTimeManager;
        }

        @Override
        public void processRecord(
                NewsEvent record, Collector<NewsClickNumber> output, PartitionedContext ctx)
                throws Exception {
            if (record.getType() == NewsEventType.RELEASE) {
                // for the news release event, record the release time and register timers
                long releaseTime = record.getTimestamp();
                releaseTimeOfNews.put(record.getNewsId(), releaseTime);
                for (Duration targetTime : TIMES_AFTER_NEWS_RELEASE) {
                    eventTimeManager.registerTimer(releaseTime + targetTime.toMillis());
                }
            } else {
                // for the news click event, record the click time
                clickTimeListOfNews
                        .computeIfAbsent(record.getNewsId(), k -> new ArrayList<>())
                        .add(record.getTimestamp());
            }
        }

        @Override
        public void onEventTimer(
                long timestamp, Collector<NewsClickNumber> output, PartitionedContext ctx) {
            // get the news that the current event timer belongs to
            long newsId = ctx.getStateManager().getCurrentKey();

            // calculate the difference between the current time and the news release time
            Duration diffTime = Duration.ofMillis(timestamp - releaseTimeOfNews.get(newsId));

            // calculate the number of clicks on the news at the current time.
            List<Long> clickTimeList = clickTimeListOfNews.get(newsId);
            long clickCount = 0;
            for (Long clickTime : clickTimeList) {
                if (clickTime <= timestamp) {
                    clickCount++;
                }
            }

            // send the result to output1
            output.collect(new NewsClickNumber(newsId, diffTime.toMillis(), clickCount));
        }
    }

    public static NonKeyedPartitionStream<NewsEvent> createSource(ExecutionEnvironment env) {
        return env.fromSource(
                        DataStreamV2SourceUtils.wrapSource(
                                new DataGeneratorSource<NewsEvent>(
                                        new NewsClickEventGenerateFunction(),
                                        Long.MAX_VALUE,
                                        TypeInformation.of(NewsEvent.class))),
                        "Source")
                .withParallelism(1);
    }
}
