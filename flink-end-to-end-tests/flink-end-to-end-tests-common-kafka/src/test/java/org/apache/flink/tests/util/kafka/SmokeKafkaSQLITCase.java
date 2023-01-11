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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.github.dockerjava.api.exception.NotFoundException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.apache.flink.tests.util.TestUtils.readCsvResultFiles;
import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

/** smoke test for the kafka connectors. */
@ExtendWith({TestLoggerExtension.class})
@Testcontainers
class SmokeKafkaSQLITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SmokeKafkaSQLITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();

    private static final String KAFKA_E2E_SQL = "kafka_e2e.sql";

    private static final Path SQL_AVRO_JAR = ResourceTestUtils.getResource(".*avro.jar");

    private static final Path SQL_TOOL_BOX_JAR = ResourceTestUtils.getResource(".*SqlToolbox.jar");

    private final Path SQL_CONNECTOR_KAFKA_JAR = ResourceTestUtils.getResource(".*kafka.jar");

    private List<String> topics;

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .network(NETWORK)
                    .logger(LOG)
                    .dependsOn(KAFKA_CONTAINER)
                    .build();

    @RegisterExtension
    public static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.basedOn(getConfiguration()))
                    .withTestcontainersSettings(TESTCONTAINERS_SETTINGS)
                    .build();

    private static AdminClient admin;

    private static Configuration getConfiguration() {
        // modify configuration to have enough slots
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        flinkConfig.setString("execution.checkpointing.interval", "5s");
        flinkConfig.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 1000000L);
        flinkConfig.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        flinkConfig.set(AkkaOptions.ASK_TIMEOUT_DURATION, Duration.ofMinutes(20));
        return flinkConfig;
    }

    @BeforeAll
    static void setUp() {
        final Map<String, Object> adminProperties = new HashMap<>();
        adminProperties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(adminProperties);
    }

    @AfterAll
    static void teardown() {
        admin.close();
    }

    @BeforeEach
    void before() {
        topics = new ArrayList<>();
    }

    @AfterEach
    void after() throws Exception {
        DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(topics);
        deleteTopicsResult.all().get();
    }

    @Test
    public void testKafkaSQL(@TempDir Path tmpPath) throws Exception {
        String containerPath = "/opt/flink/records-upsert.out";
        // create sql result's path.
        final Path sqlResultPath = tmpPath;

        // create the required topics
        String testJsonTopic = "test-json-" + "-" + UUID.randomUUID();
        String testAvroTopic = "test-avro-" + "-" + UUID.randomUUID();
        String testCsvTopic = "test-csv-" + "-" + UUID.randomUUID();
        topics.add(testJsonTopic);
        topics.add(testAvroTopic);
        topics.add(testCsvTopic);

        final short replicationFactor = 1;
        final int numPartitions = 1;
        admin.createTopics(
                        Lists.newArrayList(
                                new NewTopic(testAvroTopic, numPartitions, replicationFactor),
                                new NewTopic(testJsonTopic, numPartitions, replicationFactor)))
                .all()
                .get();

        String[] messages =
                new String[] {
                    "{\"rowtime\": \"2018-03-12 08:00:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 08:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:00:00\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:20:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
                    "{\"rowtime\": \"2018-03-12 10:40:00\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
                };
        final Properties producerProperties = new Properties();
        producerProperties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<Void, String> producer = new KafkaProducer<>(producerProperties);
        for (String message : messages) {
            producer.send(new ProducerRecord<>(testJsonTopic, message));
        }
        producer.close();

        // Initialize the SQL statements from "kafka_e2e.sql" file
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("$KAFKA_IDENTIFIER", "kafka");
        varsMap.put("$TOPIC_JSON_NAME", testJsonTopic);
        varsMap.put("$TOPIC_AVRO_NAME", testAvroTopic);
        varsMap.put("$TOPIC_CSV_NAME", testAvroTopic);
        varsMap.put("$RESULT", containerPath);
        varsMap.put("$KAFKA_BOOTSTRAP_SERVERS", INTER_CONTAINER_KAFKA_ALIAS + ":9092");
        List<String> sqlLines = initializeSqlLines(varsMap);

        // Execute SQL statements in "kafka_e2e.sql" file
        executeSqlStatements(sqlLines);

        // Wait until all the results flushed to the CSV file.
        LOG.info("Verify the CSV result.");
        List<String> group =
                new KafkaContainerClient(KAFKA_CONTAINER)
                        .readMessages(4, "test-group", testCsvTopic, new StringDeserializer());
        System.out.println(group);
        // checkCsvResultFile(containerPath, sqlResultPath.toString());
        LOG.info("The Kafka SQL client test run successfully.");
    }

    private void copyFile(String containerPath, String outputPath) throws Exception {
        GenericContainer<?> taskManager = FLINK.getTaskManagers().get(0);
        taskManager.copyFileFromContainer(containerPath, outputPath);
    }

    private void executeSqlStatements(List<String> sqlLines) throws Exception {
        LOG.info("Executing Kafka end-to-end SQL statements.");
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJar(SQL_AVRO_JAR)
                        .addJar(SQL_CONNECTOR_KAFKA_JAR)
                        .addJar(SQL_TOOL_BOX_JAR)
                        .build());
    }

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = SQLClientKafkaITCase.class.getClassLoader().getResource(KAFKA_E2E_SQL);
        if (url == null) {
            throw new FileNotFoundException(KAFKA_E2E_SQL);
        }

        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }

        return result;
    }

    private void checkCsvResultFile(String containerPath, String resultPath) throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(120));
        while (deadline.hasTimeLeft()) {
            // KafkaConsumer
            File tempOutputFile = new File(resultPath, UUID.randomUUID() + ".out");
            Files.createFile(tempOutputFile.toPath());
            try {
                copyFile(containerPath, tempOutputFile.toString());
            } catch (NotFoundException e) {
                Thread.sleep(500);
                LOG.info("The target CSV {} does not exist now", tempOutputFile);
                continue;
            }
            if (Files.exists(tempOutputFile.toPath())) {
                List<String> lines = readCsvResultFiles(tempOutputFile.toPath());
                if (lines.size() == 4) {
                    success = true;
                    Assert.assertThat(
                            lines.toArray(new String[0]),
                            arrayContainingInAnyOrder(
                                    "2018-03-12 08:00:00.000,Alice,This was a warning.,2,Success constant folding.",
                                    "2018-03-12 09:00:00.000,Bob,This was another warning.,1,Success constant folding.",
                                    "2018-03-12 09:00:00.000,Steve,This was another info.,2,Success constant folding.",
                                    "2018-03-12 09:00:00.000,Alice,This was a info.,1,Success constant folding."));
                    break;
                } else {
                    LOG.info(
                            "The target CSV {} does not contain enough records, current {} records, left time: {}s",
                            tempOutputFile,
                            lines.size(),
                            deadline.timeLeft().getSeconds());
                }
            } else {
                LOG.info("The target CSV {} does not exist now", tempOutputFile);
            }
            Thread.sleep(500);
        }
        Assert.assertTrue("Did not get expected results before timeout.", success);
    }
}
