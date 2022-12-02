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

package org.apache.flink.streaming.connectors.impala;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

/**
 * Utils for jdbc connectors.
 */
public class JDBCTypeConvertUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTypeConvertUtils.class);

    /**
     * Adds a record to the prepared statement.
     *
     * <p>When this method is called, the output format is guaranteed to be opened.
     *
     * <p>WARNING: this may fail when no column types specified (because a best effort approach is attempted in order to
     * insert a null value but it's not guaranteed that the JDBC driver handles PreparedStatement.setObject(pos, null))
     *
     * @param upload     The prepared statement.
     * @param typesArray The jdbc types of the row.
     * @param row        The records to add to the output.
     * @see PreparedStatement
     */
    public static void setRecordToStatement(PreparedStatement upload, int[] typesArray, Row row) throws SQLException {
        setRecordToStatement(upload, typesArray, row, null);
    }

    public static void setRecordToStatement(PreparedStatement upload, int[] typesArray, Row row, int[] pkFieldIndices) throws SQLException {
        if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
            LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }
        if (typesArray == null) {
            // no types provided
            for (int index = 0; index < row.getArity(); index++) {
                LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
                upload.setObject(index + 1, row.getField(index));
            }
        } else {
            // types provided
            int placeIndex = 0;
            for (int i = 0; i < row.getArity(); i++) {
                if (isPrimaryKeyField(pkFieldIndices, i)) {
                    continue;
                }
                setField(upload, typesArray[i], row.getField(i), placeIndex++);
            }

            //填充whereClause中的主键占位符
            if (pkFieldIndices != null && pkFieldIndices.length > 0) {
                for (int j = 0; j < pkFieldIndices.length; j++) {
                    int pkIndex = pkFieldIndices[j];
                    setField(upload, typesArray[pkIndex], row.getField(pkIndex), placeIndex + j);
                }
            }
        }
    }

    private static boolean isPrimaryKeyField(int[] pkFieldIndices, int fieldIndex) {
        if (pkFieldIndices == null || pkFieldIndices.length <= 0) {
            return false;
        }
        for (int index : pkFieldIndices) {
            if (index == fieldIndex) {
                return true;
            }
        }
        return false;
    }

    public static void setField(PreparedStatement upload, int type, Object field, int index) throws SQLException {
        if (field == null) {
            upload.setNull(index + 1, type);
        } else {
            try {
                // casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
                switch (type) {
                    case Types.NULL:
                        upload.setNull(index + 1, type);
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        upload.setBoolean(index + 1, (boolean) field);
                        break;
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        upload.setString(index + 1, (String) field);
                        break;
                    case Types.TINYINT:
                        upload.setByte(index + 1, (byte) field);
                        break;
                    case Types.SMALLINT:
                        upload.setShort(index + 1, (short) field);
                        break;
                    case Types.INTEGER:
                        upload.setInt(index + 1, (int) field);
                        break;
                    case Types.BIGINT:
                        upload.setLong(index + 1, (long) field);
                        break;
                    case Types.REAL:
                        upload.setFloat(index + 1, (float) field);
                        break;
                    case Types.FLOAT:
                    case Types.DOUBLE:
                        upload.setDouble(index + 1, Double.parseDouble(field.toString()));
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        upload.setBigDecimal(index + 1, (BigDecimal) field);
                        break;
                    case Types.DATE:
                        upload.setDate(index + 1, (Date) field);
                        break;
                    case Types.TIME:
                        upload.setTime(index + 1, (Time) field);
                        break;
                    case Types.TIMESTAMP:
                        upload.setTimestamp(index + 1, (Timestamp) field);
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        upload.setBytes(index + 1, (byte[]) field);
                        break;
                    case Types.CLOB:
                    case Types.NCLOB:
                        try (StringReader reader = new StringReader(field.toString())) {
                            upload.setClob(index + 1, reader);
                        }
                        break;
                    default:
                        upload.setObject(index + 1, field);
                        LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
                                type, index + 1, field);
                        // case java.sql.Types.SQLXML
                        // case java.sql.Types.ARRAY:
                        // case java.sql.Types.JAVA_OBJECT:
                        // case java.sql.Types.BLOB:
//                         case java.sql.Types.NCLOB:
                        // case java.sql.Types.DATALINK:
                        // case java.sql.Types.DISTINCT:
                        // case java.sql.Types.OTHER:
                        // case java.sql.Types.REF:
                        // case java.sql.Types.ROWID:
                        // case java.sql.Types.STRUC
                }
            } catch (ClassCastException e) {
                // enrich the exception with detailed information.
                String errorMessage = String.format(
                        "%s, field index: %s, field value: %s.", e.getMessage(), index, field);
                ClassCastException enrichedException = new ClassCastException(errorMessage);
                enrichedException.setStackTrace(e.getStackTrace());
                throw enrichedException;
            }
        }
    }

    /**
     * according to Java type, get the corresponding SQL type
     *
     * @param fieldTypeList the Java type
     * @return the type number of the corresponding type
     */
    public static int[] getSqlTypeFromFieldType(List<String> fieldTypeList) {
        int[] tmpFieldsType = new int[fieldTypeList.size()];
        for (int i = 0; i < fieldTypeList.size(); i++) {
            String fieldType = fieldTypeList.get(i).toUpperCase();
            switch (fieldType) {
                case "INT":
                    tmpFieldsType[i] = Types.INTEGER;
                    break;
                case "BOOLEAN":
                    tmpFieldsType[i] = Types.BOOLEAN;
                    break;
                case "BIGINT":
                    tmpFieldsType[i] = Types.BIGINT;
                    break;
                case "SHORT":
                    tmpFieldsType[i] = Types.SMALLINT;
                    break;
                case "STRING":
                case "CHAR":
                    tmpFieldsType[i] = Types.CHAR;
                    break;
                case "BYTE":
                    tmpFieldsType[i] = Types.BINARY;
                    break;
                case "FLOAT":
                    tmpFieldsType[i] = Types.FLOAT;
                    break;
                case "DOUBLE":
                    tmpFieldsType[i] = Types.DOUBLE;
                    break;
                case "TIMESTAMP":
                    tmpFieldsType[i] = Types.TIMESTAMP;
                    break;
                case "BIGDECIMAL":
                    tmpFieldsType[i] = Types.DECIMAL;
                    break;
                case "DATE":
                    tmpFieldsType[i] = Types.DATE;
                    break;
                case "TIME":
                    tmpFieldsType[i] = Types.TIME;
                    break;
                default:
                    throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
            }
        }
        return tmpFieldsType;
    }

    /**
     * By now specified class type conversion.
     * FIXME Follow-up has added a new type of time needs to be modified
     *
     * @param fieldTypeArray
     */
    public static int[] buildSqlTypes(List<Class> fieldTypeArray) {

        int[] tmpFieldsType = new int[fieldTypeArray.size()];
        for (int i = 0; i < fieldTypeArray.size(); i++) {
            String fieldType = fieldTypeArray.get(i).getName();
            if (fieldType.equals(Integer.class.getName())) {
                tmpFieldsType[i] = Types.INTEGER;
            } else if (fieldType.equals(Boolean.class.getName())) {
                tmpFieldsType[i] = Types.BOOLEAN;
            } else if (fieldType.equals(Long.class.getName())) {
                tmpFieldsType[i] = Types.BIGINT;
            } else if (fieldType.equals(Byte.class.getName())) {
                tmpFieldsType[i] = Types.TINYINT;
            } else if (fieldType.equals(Short.class.getName())) {
                tmpFieldsType[i] = Types.SMALLINT;
            } else if (fieldType.equals(String.class.getName())) {
                tmpFieldsType[i] = Types.CHAR;
            } else if (fieldType.equals(Float.class.getName())) {
                tmpFieldsType[i] = Types.FLOAT;
            } else if (fieldType.equals(Double.class.getName())) {
                tmpFieldsType[i] = Types.DOUBLE;
            } else if (fieldType.equals(Timestamp.class.getName())) {
                tmpFieldsType[i] = Types.TIMESTAMP;
            } else if (fieldType.equals(BigDecimal.class.getName())) {
                tmpFieldsType[i] = Types.DECIMAL;
            } else if (fieldType.equals(Date.class.getName())) {
                tmpFieldsType[i] = Types.DATE;
            } else if (fieldType.equals(Time.class.getName())) {
                tmpFieldsType[i] = Types.TIME;
            }else if (fieldType.equals(Clob.class.getName())){
                tmpFieldsType[i] = Types.CLOB;
            } else if (fieldType.equals(NClob.class.getName())) {
                tmpFieldsType[i] = Types.NCLOB;
            } else {
                throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
            }
        }

        return tmpFieldsType;
    }

}
