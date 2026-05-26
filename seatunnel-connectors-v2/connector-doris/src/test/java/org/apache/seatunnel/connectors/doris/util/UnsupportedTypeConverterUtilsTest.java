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
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
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
                        new SeaTunnelDataType<?>[] {
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
    public void testConvertVectorFieldsPreservesRowKind() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        Float[] floatArray = {1.0f, 2.0f};
        ByteBuffer buffer = VectorUtils.toByteBuffer(floatArray);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, buffer});
        row.setRowKind(RowKind.DELETE);
        row.setTableId("test_table");

        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        Assertions.assertEquals(RowKind.DELETE, converted.getRowKind());
        Assertions.assertEquals("test_table", converted.getTableId());
        Assertions.assertArrayEquals(floatArray, (Float[]) converted.getField(1));
    }

    @Test
    public void testConvertVectorFieldsFloat16Vector() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_FLOAT16_TYPE
                        });

        // 1.0f -> half-precision 0x3C00, 2.0f -> 0x4000
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0x3C00);
        buffer.putShort((short) 0x4000);
        buffer.flip();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, buffer});
        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        Float[] result = (Float[]) converted.getField(1);
        Assertions.assertEquals(2, result.length);
        Assertions.assertEquals(1.0f, result[0], 0.001f);
        Assertions.assertEquals(2.0f, result[1], 0.001f);
    }

    @Test
    public void testConvertVectorFieldsBFloat16Vector() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_BFLOAT16_TYPE
                        });

        // BFLOAT16 = top 16 bits of float32
        // 3.0f = 0x40400000 -> BFLOAT16 0x4040, 4.0f = 0x40800000 -> BFLOAT16 0x4080
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0x4040);
        buffer.putShort((short) 0x4080);
        buffer.flip();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, buffer});
        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        Float[] result = (Float[]) converted.getField(1);
        Assertions.assertEquals(2, result.length);
        Assertions.assertEquals(3.0f, result[0], 0.001f);
        Assertions.assertEquals(4.0f, result[1], 0.001f);

        // BFLOAT16 precision loss: 1.1f = 0x3F8CCCCD -> BFLOAT16 0x3F8C -> 0x3F8C0000 = 1.09375f
        // 7 mantissa bits lose ~16 bits of precision compared to float32
        buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 0x3F8C);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {1, buffer});
        converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);
        result = (Float[]) converted.getField(1);
        Assertions.assertEquals(1, result.length);
        Assertions.assertEquals(1.09375f, result[0]);
        Assertions.assertNotEquals(1.1f, result[0], "BFLOAT16 should lose precision for 1.1f");
    }

    @Test
    public void testConvertVectorFieldsNoVectorColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

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
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, VectorType.VECTOR_SPARSE_FLOAT_TYPE
                        });

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, new HashMap<Integer, Float>()});
        SeaTunnelRow converted = UnsupportedTypeConverterUtils.convertVectorFields(rowType, row);

        Assertions.assertSame(row, converted);
    }

    @Test
    public void testConvertCatalogTableWithVectorColumns() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder().name("id").dataType(BasicType.INT_TYPE).build(),
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
                        PhysicalColumn.builder().name("id").dataType(BasicType.INT_TYPE).build(),
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

    @Test
    public void testConvertRowTypeWithVectorColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "embedding", "embedding16", "name"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            VectorType.VECTOR_FLOAT_TYPE,
                            VectorType.VECTOR_FLOAT16_TYPE,
                            BasicType.STRING_TYPE
                        });

        SeaTunnelRowType converted = UnsupportedTypeConverterUtils.convertRowType(rowType);
        Assertions.assertEquals(BasicType.INT_TYPE, converted.getFieldTypes()[0]);
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, converted.getFieldTypes()[1]);
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, converted.getFieldTypes()[2]);
        Assertions.assertEquals(BasicType.STRING_TYPE, converted.getFieldTypes()[3]);
        Assertions.assertArrayEquals(
                new String[] {"id", "embedding", "embedding16", "name"}, converted.getFieldNames());
    }

    @Test
    public void testConvertRowTypeWithoutVectorColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRowType converted = UnsupportedTypeConverterUtils.convertRowType(rowType);
        Assertions.assertSame(rowType, converted);
    }

    @Test
    public void testDecodeFloat16SpecialValues() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"v"},
                        new SeaTunnelDataType<?>[] {VectorType.VECTOR_FLOAT16_TYPE});

        // Test zero: +0.0 = 0x0000, -0.0 = 0x8000
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0x0000);
        buffer.putShort((short) 0x8000);
        buffer.flip();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {buffer});
        Float[] result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(0.0f, result[0]);
        Assertions.assertEquals(Float.floatToRawIntBits(-0.0f), Float.floatToRawIntBits(result[1]));

        // Test infinity: +inf = 0x7C00, -inf = 0xFC00
        buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0x7C00);
        buffer.putShort((short) 0xFC00);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {buffer});
        result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(Float.POSITIVE_INFINITY, result[0]);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, result[1]);

        // Test NaN: 0x7E00
        buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 0x7E00);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {buffer});
        result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertTrue(Float.isNaN(result[0]));

        // Test denormalized: 0x0400 = 2^(-14) × (1024/1024) = 2^(-14) ≈ 6.10e-5
        buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 0x0400);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {buffer});
        result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(6.103515625e-5f, result[0], 1e-12f);

        // Test smallest denormalized: 0x0001 = 2^(-14) × (1/1024) = 2^(-24) ≈ 5.96e-8
        buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 0x0001);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {buffer});
        result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(5.9604645e-8f, result[0], 1e-15f);
    }

    @Test
    public void testDecodeBFloat16() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"v"},
                        new SeaTunnelDataType<?>[] {VectorType.VECTOR_BFLOAT16_TYPE});

        // BFLOAT16 1.0f = top 16 bits of 0x3F800000 = 0x3F80
        // BFLOAT16 2.0f = top 16 bits of 0x40000000 = 0x4000
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0x3F80);
        buffer.putShort((short) 0x4000);
        buffer.flip();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {buffer});
        Float[] result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(2, result.length);
        Assertions.assertEquals(1.0f, result[0]);
        Assertions.assertEquals(2.0f, result[1]);

        // BFLOAT16 special values: +0.0 = 0x0000, -0.0 = 0x8000
        buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0x0000);
        buffer.putShort((short) 0x8000);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {buffer});
        result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(0.0f, result[0]);
        Assertions.assertEquals(Float.floatToRawIntBits(-0.0f), Float.floatToRawIntBits(result[1]));

        // BFLOAT16 +inf = 0x7F80, -inf = 0xFF80, NaN = 0x7FC0
        buffer = ByteBuffer.allocate(6);
        buffer.putShort((short) 0x7F80);
        buffer.putShort((short) 0xFF80);
        buffer.putShort((short) 0x7FC0);
        buffer.flip();
        row = new SeaTunnelRow(new Object[] {buffer});
        result =
                (Float[])
                        UnsupportedTypeConverterUtils.convertVectorFields(rowType, row).getField(0);
        Assertions.assertEquals(Float.POSITIVE_INFINITY, result[0]);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, result[1]);
        Assertions.assertTrue(Float.isNaN(result[2]));
    }
}
