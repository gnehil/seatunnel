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

package org.apache.seatunnel.connectors.doris.sink.savemode;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;

public class DorisSaveModeHandler implements SaveModeHandler {

    private SchemaSaveMode schemaSaveMode;

    private DataSaveMode dataSaveMode;

    private DorisCatalog catalog;

    private TablePath tablePath;

    private CatalogTable catalogTable;

    private String customSql;

    public DorisSaveModeHandler(SchemaSaveMode schemaSaveMode, DataSaveMode dataSaveMode, DorisCatalog catalog,
                                TablePath tablePath, CatalogTable catalogTable, String customSql) {
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.catalog = catalog;
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.customSql = customSql;
    }

    @Override
    public void handleSchemaSaveMode() {

        switch (schemaSaveMode) {
            case RECREATE_SCHEMA:
                catalog.dropTable(tablePath, true);
                catalog.createTable(tablePath, catalogTable, false);
                break;
            case CREATE_SCHEMA_WHEN_NOT_EXIST:
                if (!catalog.tableExists(tablePath)) {
                    catalog.createTable(tablePath, catalogTable, false);
                }
                break;
            case ERROR_WHEN_SCHEMA_NOT_EXIST:
                if (!catalog.tableExists(tablePath)) {
                    String msg = String.format("Table [%s] is not exists.", tablePath.getFullName());
                    throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST, msg);
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported schema save mode: %s", schemaSaveMode));
        }

    }

    @Override
    public void handleDataSaveMode() {

        switch (dataSaveMode) {
            case DROP_DATA:
                catalog.truncateTable(tablePath, true);
                break;
            case APPEND_DATA:
                // do nothing
                break;
            case ERROR_WHEN_DATA_EXISTS:
                if (catalog.isExistsData(tablePath)) {
                    String msg = String.format("Table [%s] has data", tablePath.getFullName());
                    throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA, msg);
                }
                break;
            case CUSTOM_PROCESSING:
                catalog.executeSql(tablePath, customSql);
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported data save mode: %s", dataSaveMode));
        }

    }

    @Override
    public void close() throws Exception {

    }

}
