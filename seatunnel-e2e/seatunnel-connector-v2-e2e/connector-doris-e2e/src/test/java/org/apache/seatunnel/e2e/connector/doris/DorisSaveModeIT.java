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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.Collections;

public class DorisSaveModeIT extends DorisTestBase implements TestResource {

    private GenericContainer<?> container;

    private DorisCatalog catalog;

    private TablePath tablePath;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        container = createContainer();
        startContainer(container);
        initCatalog();
        tablePath = TablePath.of("test", "t_test");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (container != null && container.isCreated()) {
            container.close();
        }
        if (catalog != null) {
            catalog.close();
        }
    }

    private void initCatalog() {
        if (catalog == null) {
            catalog =
                    new DorisCatalog(
                            "Doris",
                            container.getHost() + ":" + HTTP_PORT,
                            QUERY_PORT,
                            USERNAME,
                            PASSWORD);
        }
        catalog.open();
    }

    @TestTemplate
    public void recreateSchemaTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/save-mode-recreate-test.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void createSchemaWhenNotExistTest(TestContainer container)
            throws IOException, InterruptedException {
        catalog.dropTable(tablePath, true);
        Container.ExecResult execResult = container.executeJob("/save-mode-create-test.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        catalog.tableExists(tablePath);
    }

    @TestTemplate
    public void errorWhenSchemaNotExistTest(TestContainer container)
            throws IOException, InterruptedException {
        catalog.dropTable(tablePath, true);
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> container.executeJob("/save-mode-notexist-test.conf"));

        TableIdentifier tableIdentifier = TableIdentifier.of("Doris", tablePath);
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("a", BasicType.INT_TYPE, 10, true, null, ""))
                        .column(PhysicalColumn.of("b", BasicType.STRING_TYPE, 54, true, null, ""))
                        .column(PhysicalColumn.of("c", BasicType.DOUBLE_TYPE, 10, true, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "d", LocalTimeType.LOCAL_DATE_TYPE, 10, true, null, ""))
                        .build();
        catalog.createTable(
                tablePath,
                CatalogTable.of(tableIdentifier, schema, null, Collections.singletonList("a"), ""),
                true);
        Container.ExecResult execResult = container.executeJob("/save-mode-notexist-test.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
