package org.apache.flink.streaming.connectors.greenplum.data;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/12/05 17:28
 */
public class BuildData {
    private static final char FIELD_DELIMITER = '|';
    private static final char NEWLINE = '\n';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';
    private static int MaxCsvSize = 4194304;

    private static final Logger LOG = LoggerFactory.getLogger(BuildData.class);

    private int columnNumber;
    private Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    public BuildData(int columnNumber,
                     Triple<List<String>, List<Integer>, List<String>> resultSetMetaData
    ) {
        this.columnNumber = columnNumber;
        this.resultSetMetaData = resultSetMetaData;
    }


    public byte[] serializeRecord(RowData record) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.columnNumber; i++) {
            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
            if (record.isNullAt(i)) {
                if (i + 1 < this.columnNumber) {
                    sb.append(FIELD_DELIMITER);
                }
                continue;
            }

            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    String data = record.getString(i).toString();
                    if (data != null) {
                        sb.append(QUOTE);
                        sb.append(escapeString(data));
                        sb.append(QUOTE);
                    }
                    break;
                }
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                case Types.LONGVARBINARY:
                case Types.NCLOB:
                case Types.VARBINARY: {
                    byte[] data = record.getString(i).toBytes();
                    if (data != null) {
                        sb.append(escapeBinary(data));
                    }
                    break;
                }
                case Types.TINYINT:
                case Types.SMALLINT: {
                    int anInt = record.getInt(i);
                    if (!Objects.isNull(anInt)) {
                        sb.append(anInt);
                    }
                    break;
                }
                case Types.INTEGER:
                case Types.BIGINT: {
                    long aLong = record.getLong(i);
                    if (!Objects.isNull(aLong)) {
                        sb.append(aLong);
                    }
                    break;
                }
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.REAL: {
                    double aDouble = record.getDouble(i);
                    if (!Objects.isNull(aDouble)) {
                        sb.append(aDouble);
                    }
                    break;
                }
                default: {
                    String data = record.getString(i).toString();
                    if (data != null) {
                        sb.append(data);
                    }

                    break;
                }
            }

            if (i + 1 < this.columnNumber) {
                sb.append(FIELD_DELIMITER);
            }
        }
        sb.append(NEWLINE);
        return sb.toString().getBytes("UTF-8");
    }

    /**
     * Any occurrence within the value of a QUOTE character or the ESCAPE
     * character is preceded by the escape character.
     */
    protected String escapeString(String data) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length(); ++i) {
            char c = data.charAt(i);
            switch (c) {
                case 0x00:
                    LOG.warn("字符串中发现非法字符 0x00，已经将其删除");
                    continue;
                case QUOTE:
                case ESCAPE:
                    sb.append(ESCAPE);
            }

            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Non-printable characters are inserted as '\nnn' (octal) and '\' as '\\'.
     */
    protected String escapeBinary(byte[] data) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length; ++i) {
            if (data[i] == '\\') {
                sb.append('\\');
                sb.append('\\');
            } else if (data[i] < 0x20 || data[i] > 0x7e) {
                byte b = data[i];
                char[] val = new char[3];
                val[2] = (char) ((b & 07) + '0');
                b >>= 3;
                val[1] = (char) ((b & 07) + '0');
                b >>= 3;
                val[0] = (char) ((b & 03) + '0');
                sb.append('\\');
                sb.append(val);
            } else {
                sb.append((char) (data[i]));
            }
        }

        return sb.toString();
    }

}
