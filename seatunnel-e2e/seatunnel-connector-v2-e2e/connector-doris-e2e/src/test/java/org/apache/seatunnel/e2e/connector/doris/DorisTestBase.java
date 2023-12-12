/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public class DorisTestBase extends TestSuiteBase {

    public static final Logger LOG = LoggerFactory.getLogger(DorisTestBase.class);

    // use image adamlee489/doris:1.2.7.1_arm when running this test on mac
    // private static final String DOCKER_IMAGE = "adamlee489/doris:1.2.7.1_x86";
    protected static final String DOCKER_IMAGE = "adamlee489/doris:1.2.7.1_arm";
    protected static final String HOST = "doris_catalog_e2e";
    protected static final int DOCKER_QUERY_PORT = 9030;
    protected static final int DOCKER_HTTP_PORT = 8030;
    protected static final int QUERY_PORT = 19030;
    protected static final int HTTP_PORT = 18030;
    protected static final String URL = "jdbc:mysql://%s:" + QUERY_PORT;
    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "";
    protected static final String DATABASE = "test";
    protected static final String SINK_TABLE = "doris_catalog_e2e";
    private static final String SET_SQL =
            "ADMIN SET FRONTEND CONFIG (\"enable_batch_delete_by_default\" = \"true\")";
    private static final String SHOW_BE = "SHOW BACKENDS";

    protected GenericContainer<?> createContainer() {

        GenericContainer<?> container =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withEnv("FE_SERVERS", "fe1:127.0.0.1:9010")
                        .withEnv("FE_ID", "1")
                        .withEnv("CURRENT_BE_IP", "127.0.0.1")
                        .withEnv("CURRENT_BE_PORT", "9050")
                        .withCommand("ulimit -n 65536")
                        .withCreateContainerCmdModifier(
                                cmd -> cmd.getHostConfig().withMemorySwap(0L))
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", QUERY_PORT, DOCKER_QUERY_PORT),
                        String.format("%s:%s", HTTP_PORT, DOCKER_HTTP_PORT)));

        return container;
    }

    protected void startContainer(GenericContainer<?> container) {
        Startables.deepStart(Stream.of(container)).join();
        LOG.info("doris container started");
        given().ignoreExceptions()
                .await()
                .atMost(10000, TimeUnit.SECONDS)
                .untilAsserted(() -> initializeJdbcConnection(container));
    }

    protected void initializeJdbcConnection(GenericContainer<?> container) throws SQLException {
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);

        try (Connection conn =
                        DriverManager.getConnection(
                                String.format(URL, container.getHost()), props);
                Statement statement = conn.createStatement()) {
            statement.execute(SET_SQL);
            ResultSet resultSet;
            do {
                resultSet = statement.executeQuery(SHOW_BE);
            } while (!isBeReady(resultSet, Duration.ofSeconds(1L)));
        }
    }

    private boolean isBeReady(ResultSet rs, Duration duration) throws SQLException {
        if (rs.next()) {
            String isAlive = rs.getString(10).trim();
            String totalCap = rs.getString(16).trim();
            LockSupport.parkNanos(duration.toNanos());
            return "true".equalsIgnoreCase(isAlive) && !"0.000".equalsIgnoreCase(totalCap);
        }
        return false;
    }
}
