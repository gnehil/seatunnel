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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class UnsupportedTypeConverterUtilsTest {

    @Test
    public void testConvertVectorFieldsFloatVector() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        Float[] floatArray = {1.0f, 2.0f, 3.0f};
        ByteBuffer buffer = VectorUtils.toByteBuffer(floatArray);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, buffer});

        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        Assertions.assertArrayEquals(floatArray, (Float[]) converted.getField(1));
        Assertions.assertEquals(1, converted.getField(0));
    }

    @Test
    public void testConvertVectorFieldsFloat16Vector() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_FLOAT16_TYPE
                        });

        Float[] floatArray = {1.0f, 2.0f};
        ByteBuffer buffer = VectorUtils.toByteBuffer(floatArray);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, buffer});

        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        Assertions.assertArrayEquals(floatArray, (Float[]) converted.getField(1));
    }

    @Test
    public void testConvertVectorFieldsBFloat16Vector() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_BFLOAT16_TYPE
                        });

        Float[] floatArray = {3.0f, 4.0f};
        ByteBuffer buffer = VectorUtils.toByteBuffer(floatArray);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, buffer});

        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        Assertions.assertArrayEquals(floatArray, (Float[]) converted.getField(1));
    }

    @Test
    public void testConvertVectorFieldsNoVectorColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test"});
        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);

        Assertions.assertSame(row, converted);
        Assertions.assertEquals(1, converted.getField(0));
        Assertions.assertEquals("test", converted.getField(1));
    }

    @Test
    public void testConvertVectorFieldsUnsupportedVectorTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "sparse"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_SPARSE_FLOAT_TYPE
                        });

        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {1, new HashMap<Integer, Float>()});
        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);

        Assertions.assertSame(row, converted);
    }

    @Test
    public void testConvertCatalogTableWithVectorColumns() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder()
                                .name("id")
                                .dataType(BasicType.INT_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("embedding")
                                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("embedding16")
                                .dataType(VectorType.VECTOR_FLOAT16_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("name")
                                .dataType(BasicType.STRING_TYPE)
                                .build());

        TableSchema tableSchema = TableSchema.builder().columns(columns).build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");

        CatalogTable converted = UnsupportedTypeConverterUtils.convertCatalogTable(catalogTable);
        List<Column> convertedColumns = converted.getTableSchema().getColumns();

        Assertions.assertEquals(BasicType.INT_TYPE, convertedColumns.get(0).getDataType());
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, convertedColumns.get(1).getDataType());
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, convertedColumns.get(2).getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, convertedColumns.get(3).getDataType());
        Assertions.assertEquals("ARRAY<FLOAT>", convertedColumns.get(1).getSourceType());
        Assertions.assertEquals("ARRAY<FLOAT>", convertedColumns.get(2).getSourceType());
    }

    @Test
    public void testConvertCatalogTableWithoutVectorColumns() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder()
                                .name("id")
                                .dataType(BasicType.INT_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("name")
                                .dataType(BasicType.STRING_TYPE)
                                .build());

        TableSchema tableSchema = TableSchema.builder().columns(columns).build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");

        CatalogTable converted = UnsupportedTypeConverterUtils.convertCatalogTable(catalogTable);
        List<Column> convertedColumns = converted.getTableSchema().getColumns();

        Assertions.assertEquals(BasicType.INT_TYPE, convertedColumns.get(0).getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, convertedColumns.get(1).getDataType());
    }
}
