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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.VectorUtils;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;

@Slf4j
public class UnsupportedTypeConverterUtils {
    public static Object convertBigDecimal(BigDecimal bigDecimal) {
        if (bigDecimal.precision() > 38) {
            return bigDecimal.doubleValue();
        }
        return bigDecimal;
    }

    public static SeaTunnelRow convertRow(SeaTunnelRow row) {
        List<Object> newValues =
                Arrays.stream(row.getFields())
                        .map(
                                value -> {
                                    if (value instanceof BigDecimal) {
                                        return convertBigDecimal((BigDecimal) value);
                                    }
                                    return value;
                                })
                        .collect(Collectors.toList());
        return new SeaTunnelRow(newValues.toArray());
    }

    public static SeaTunnelRow convertVectorFields(
            SeaTunnelRowType rowType, SeaTunnelRow row) {
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();
        Object[] fields = null;
        for (int i = 0; i < fieldTypes.length; i++) {
            SqlType sqlType = fieldTypes[i].getSqlType();
            if (row.getField(i) instanceof ByteBuffer
                    && (sqlType == SqlType.FLOAT_VECTOR
                            || sqlType == SqlType.FLOAT16_VECTOR
                            || sqlType == SqlType.BFLOAT16_VECTOR)) {
                if (fields == null) {
                    fields = row.getFields().clone();
                }
                ByteBuffer buffer = (ByteBuffer) row.getField(i);
                if (sqlType == SqlType.FLOAT_VECTOR) {
                    fields[i] = VectorUtils.toFloatArray(buffer);
                } else {
                    fields[i] = decodeHalfPrecisionVector(buffer);
                }
                log.debug(
                        "Converted vector field '{}' from {} to float array",
                        rowType.getFieldName(i),
                        sqlType);
            }
        }
        if (fields != null) {
            return new SeaTunnelRow(fields);
        }
        return row;
    }

    /**
     * Decode a ByteBuffer containing half-precision (2-byte) floats to a Float array of
     * single-precision (4-byte) floats. Used for FLOAT16_VECTOR and BFLOAT16_VECTOR.
     */
    private static Float[] decodeHalfPrecisionVector(ByteBuffer buffer) {
        int numElements = buffer.remaining() / 2;
        Float[] result = new Float[numElements];
        for (int i = 0; i < numElements; i++) {
            int halfBits = buffer.getShort() & 0xFFFF;
            int sign = (halfBits >> 15) & 0x1;
            int exponent = (halfBits >> 10) & 0x1F;
            int mantissa = halfBits & 0x3FF;
            int floatBits;
            if (exponent == 0) {
                if (mantissa == 0) {
                    floatBits = sign << 31;
                } else {
                    int e = -1;
                    int m = mantissa;
                    while ((m & 0x400) == 0) {
                        m <<= 1;
                        e--;
                    }
                    m &= 0x3FF;
                    floatBits = (sign << 31) | ((e + 127) << 23) | (m << 13);
                }
            } else if (exponent == 31) {
                if (mantissa == 0) {
                    floatBits = (sign << 31) | 0x7F800000;
                } else {
                    floatBits = (sign << 31) | 0x7FC00000;
                }
            } else {
                floatBits =
                        (sign << 31) | ((exponent - 15 + 127) << 23) | (mantissa << 13);
            }
            result[i] = Float.intBitsToFloat(floatBits);
        }
        return result;
    }

    /**
     * Convert vector types in a SeaTunnelRowType to ARRAY<FLOAT>. Used for schema evolution where
     * the row type needs to be converted before creating a serializer.
     */
    public static SeaTunnelRowType convertRowType(SeaTunnelRowType rowType) {
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();
        SeaTunnelDataType<?>[] newTypes = null;
        for (int i = 0; i < fieldTypes.length; i++) {
            SqlType sqlType = fieldTypes[i].getSqlType();
            if (sqlType == SqlType.FLOAT_VECTOR
                    || sqlType == SqlType.FLOAT16_VECTOR
                    || sqlType == SqlType.BFLOAT16_VECTOR) {
                if (newTypes == null) {
                    newTypes = fieldTypes.clone();
                }
                newTypes[i] = ArrayType.FLOAT_ARRAY_TYPE;
            }
        }
        if (newTypes != null) {
            return new SeaTunnelRowType(rowType.getFieldNames(), newTypes);
        }
        return rowType;
    }

    public static CatalogTable convertCatalogTable(CatalogTable catalogTable) {
        TableSchema tableSchema = catalogTable.getTableSchema();
        List<Column> columns = tableSchema.getColumns();
        List<Column> newColumns =
                columns.stream()
                        .map(
                                column -> {
                                    if (column.getDataType().getSqlType().equals(SqlType.DECIMAL)) {
                                        DecimalType decimalType =
                                                (DecimalType) column.getDataType();
                                        if (decimalType.getPrecision() > 38) {
                                            return PhysicalColumn.of(
                                                    column.getName(),
                                                    DOUBLE_TYPE,
                                                    22,
                                                    column.isNullable(),
                                                    null,
                                                    column.getComment(),
                                                    "DOUBLE",
                                                    false,
                                                    false,
                                                    0L,
                                                    column.getOptions(),
                                                    22L);
                                        }
                                    }
                                    SqlType sqlType = column.getDataType().getSqlType();
                                    if (sqlType == SqlType.FLOAT_VECTOR
                                            || sqlType == SqlType.FLOAT16_VECTOR
                                            || sqlType == SqlType.BFLOAT16_VECTOR) {
                                        return PhysicalColumn.of(
                                                column.getName(),
                                                ArrayType.FLOAT_ARRAY_TYPE,
                                                column.getColumnLength(),
                                                column.isNullable(),
                                                column.getDefaultValue(),
                                                column.getComment(),
                                                "ARRAY<FLOAT>",
                                                false,
                                                false,
                                                0L,
                                                column.getOptions(),
                                                column.getSourceType());
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());
        TableSchema newtableSchema =
                TableSchema.builder()
                        .columns(newColumns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                newtableSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName());
    }
}
