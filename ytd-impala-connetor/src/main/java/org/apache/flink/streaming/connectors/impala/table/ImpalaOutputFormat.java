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

package org.apache.flink.streaming.connectors.impala.table;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.impala.JDBCTypeConvertUtils;
import org.apache.flink.streaming.connectors.impala.factory.DTThreadFactory;
import org.apache.flink.streaming.connectors.impala.outputformat.AbstractDtRichOutputFormat;
import org.apache.flink.streaming.connectors.impala.util.DtStringUtil;
import org.apache.flink.streaming.connectors.impala.util.JdbcConnectionUtil;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.impala.JDBCTypeConvertUtils.setRecordToStatement;


public class ImpalaOutputFormat<IN> extends AbstractDtRichOutputFormat<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(ImpalaOutputFormat.class);

    private static final long serialVersionUID = 1L;

    // ${field}
    private static final Pattern STATIC_PARTITION_PATTERN = Pattern.compile("\\$\\{([^}]*)}");
    //specific type which values need to be quoted
    private static final String[] NEED_QUOTE_TYPE = {"string", "timestamp", "varchar"};

    private static final Integer DEFAULT_CONN_TIME_OUT = 60;
    private static final int RECEIVE_DATA_PRINT_FREQUENCY = 1000;
    private static final int DIRTY_DATA_PRINT_FREQUENCY = 1000;

    private static final String KUDU_TYPE = "kudu";
    private static final String UPDATE_MODE = "update";
    private static final String PARTITION_CONSTANT = "PARTITION";
    private static final String DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

    private static final String VALUES_CONDITION = "${valuesCondition}";
    private static final String PARTITION_CONDITION = "${partitionCondition}";
    private static final String TABLE_FIELDS_CONDITION = "${tableFieldsCondition}";
    private static final String NO_PARTITION = "noPartition";

    // partition field of static partition which matched by ${field}
    private final List<String> staticPartitionFields = new ArrayList<>();
    public List<String> fieldNames;
    public List<String> fieldTypes;
    protected transient Connection connection;
    protected transient Statement statement;
    protected transient PreparedStatement updateStatement;
    protected String keytabPath;
    protected String krb5confPath;
    protected String principal;
    protected Integer authMech;
    protected String dbUrl;
    protected String userName;
    protected String password;
    protected int batchSize = 100;
    protected long batchWaitInterval = 60 * 1000L;
    protected String tableName;
    protected List<String> primaryKeys;
    protected String partitionFields;
    protected Boolean enablePartition;
    protected String schema;
    protected String storeType;
    protected String updateMode;
    private transient volatile boolean closed = false;
    private int batchCount = 0;
    // |------------------------------------------------|
    // |   partitionCondition   |Array of valueCondition|
    // |------------------------------------------------|
    // | ptOne, ptTwo, ptThree  | [(v1, v2, v3, v4, v5)]|   DP
    // |------------------------------------------------|
    // | ptOne = v1, ptTwo = v2 | [(v3, v4, v5)]        |   SP
    // |------------------------------------------------|
    // | ptOne, ptTwo = v2      | [(v1, v3, v4, v5)]    |   DP and SP
    // |------------------------------------------------|
    // | noPartition            | [(v1, v2, v3, v4, v5)]|   kudu or disablePartition
    // |------------------------------------------------|
    private transient Map<String, ArrayList<String>> rowDataMap;
    // valueFieldsName -> ???????????????fieldNames???????????????row data???????????????
    // ?????????partition??????????????????????????????????????????????????????(age, name, id)???????????????partition????????????SQL???
    // INSERT INTO tableName(name, id) PARTITION(age) VALUES(?, ?, ?)
    // ????????????executeSql??????????????????????????????(name, id, age)???????????????????????????type?????????????????????
    private List<String> valueFieldNames;
    private List<Row> rows;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient AbstractDtRichOutputFormat<?> metricOutputFormat;

    private  DynamicTableSink.DataStructureConverter converter;

    public static Builder getImpalaBuilder() {
        return new Builder();
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            rowDataMap = new HashMap<>();
            rows = new ArrayList<>();
            metricOutputFormat = this;
            openConnect(); //jdbc??????
            initScheduledTask(batchWaitInterval);//??????
            init();
            initMetric();
        } catch (Exception e) {
            throw new RuntimeException("impala output format open error!", e);
        }
    }



    private void init() throws SQLException {

        if (Objects.nonNull(partitionFields)) {
            // match ${field} from partitionFields
            Matcher matcher = STATIC_PARTITION_PATTERN.matcher(partitionFields);
            while (matcher.find()) {
                LOG.info("find static partition field: {}", matcher.group(1));
                staticPartitionFields.add(matcher.group(1).split("=")[0]);
            }
        }
        existTable(partitionFields);

        if (updateMode.equalsIgnoreCase(UPDATE_MODE)) {
            if (!storeType.equalsIgnoreCase(KUDU_TYPE)) {
                throw new IllegalArgumentException("update mode not support for non-kudu table!");
            }
            updateStatement = connection.prepareStatement(buildUpdateSql(schema, tableName, fieldNames, primaryKeys));
        } else {
            valueFieldNames = rebuildFieldNameListAndTypeList(fieldNames, staticPartitionFields, fieldTypes, partitionFields);
        }
    }

    private void existTable( String partitionFields) throws SQLException {
        StringBuilder sb = new StringBuilder();
        List<String>  partitions = new ArrayList<>();
        for (String part : partitionFields.split(",")) {
            partitions.add(part.replaceAll("\\$\\{", "").replaceAll("}","").split("=")[0]);
        }
        List<String>  partitionFieldName = new ArrayList<>();
        List<String>  partitionsFileType = new ArrayList<>();

        sb.append("CREATE TABLE IF NOT EXISTS `")
                .append(schema).append("`.`").append(tableName).append("` ( \n");

        for (int i=0;i<fieldNames.size();i++){
            if(partitions.contains(fieldNames.get(i))){
                partitionFieldName.add(fieldNames.get(i));
                partitionsFileType.add(fieldTypes.get(i));
                continue;
            }
            if(i==fieldNames.size()-1){
                sb.append(fieldNames.get(i)).append("  ").append(fieldTypes.get(i)).append(" ) \n");
            }else {
                sb.append(fieldNames.get(i)).append("  ").append(fieldTypes.get(i)).append(", \n");
            }
        }
        if(enablePartition&&!StringUtils.isEmpty(partitionFields)){
            sb.append(" PARTITIONED BY (   \n");
            for (int i=0;i<partitionFieldName.size();i++){
                if(i==partitionFieldName.size()-1){
                    sb.append(partitionFieldName.get(i)).append("  ").append(partitionsFileType.get(i)).append(" ) \n");
                }else {
                    sb.append(partitionFieldName.get(i)).append("  ").append(partitionsFileType.get(i)).append(", \n");
                }
            }

        }
        sb.append(" STORED AS   ").append(storeType);
        LOG.info("create table sql: {}", sb.toString());
        statement.execute(sb.toString());
    }

    private void initScheduledTask(Long batchWaitInterval) {
        try {
            if (batchWaitInterval != 0) {
                this.scheduler = new ScheduledThreadPoolExecutor(1,
                        new DTThreadFactory("impala-upsert-output-format"));
                this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                    synchronized (ImpalaOutputFormat.this) {
                        flush();
                    }
                }, batchWaitInterval, batchWaitInterval, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void openConnect() throws IOException {
        if (authMech == 1) {
//            UserGroupInformation ugi = KrbUtils.loginAndReturnUgi(principal, keytabPath, krb5confPath);
//            try {
//                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
//                    openJdbc();
//                    return null;
//                });
//            } catch (InterruptedException | IOException e) {
//                throw new IllegalArgumentException("connect impala error!", e);
//            }
        } else {
            openJdbc();
        }
    }

    /**
     * get jdbc connection
     */
    private void openJdbc() {
        try {
            Class.forName(DRIVER_NAME);
            DriverManager.setLoginTimeout(10);
            connection = DriverManager.getConnection(dbUrl);
            statement = connection.createStatement();
            connection.setAutoCommit(false);
        } catch (SQLException | ClassNotFoundException sqlException) {
            throw new RuntimeException("get impala jdbc connection failed!", sqlException);
        }
    }

    private synchronized void flush() {
        try {
            if (batchCount > 0) {
                if (updateMode.equalsIgnoreCase(UPDATE_MODE)) {
                    executeUpdateBatch();
                }
                if (!rowDataMap.isEmpty()) {
                    String templateSql =
                        "INSERT INTO tableName ${tableFieldsCondition} PARTITION ${partitionCondition} VALUES ${valuesCondition}";
                    executeBatchSql(
                        templateSql,
                        schema,
                        tableName,
                        storeType,
                        enablePartition,
                        valueFieldNames,
                        partitionFields,
                        rowDataMap
                    );
                    rowDataMap.clear();
                }
            }
            batchCount = 0;
        } catch (Exception e) {
            LOG.error("Writing records to impala jdbc failed.", e);
            throw new RuntimeException("Writing records to impala jdbc failed.", e);
        }
    }

    /**
     * execute batch update statement
     *
     * @throws SQLException throw sql exception
     */
    private void executeUpdateBatch() throws SQLException {
        try {
            rows.forEach(row -> {
                try {
                    setRecordToStatement(
                            updateStatement,
                            JDBCTypeConvertUtils.getSqlTypeFromFieldType(fieldTypes),
                            row,
                            primaryKeys.stream().mapToInt(fieldNames::indexOf).toArray()
                    );
                    updateStatement.addBatch();
                } catch (Exception e) {
                    throw new RuntimeException("impala jdbc execute batch error!", e);
                }
            });
            updateStatement.executeBatch();
            connection.commit();
            rows.clear();
        } catch (Exception e) {
            LOG.debug("impala jdbc execute batch error ", e);
            JdbcConnectionUtil.rollBack(connection);
            JdbcConnectionUtil.commit(connection);
            updateStatement.clearBatch();
            executeUpdate(connection);
        }
    }

    public void executeUpdate(Connection connection) {
        rows.forEach(row -> {
            try {
                setRecordToStatement(updateStatement, JDBCTypeConvertUtils.getSqlTypeFromFieldType(fieldTypes), row);
                updateStatement.executeUpdate();
                JdbcConnectionUtil.commit(connection);
            } catch (Exception e) {
                JdbcConnectionUtil.rollBack(connection);
                JdbcConnectionUtil.commit(connection);
                if (metricOutputFormat.outDirtyRecords.getCount() % DIRTY_DATA_PRINT_FREQUENCY == 0 || LOG.isDebugEnabled()) {
                    LOG.error("record insert failed ,this row is {}", row.toString());
                    LOG.error("", e);
                }
                metricOutputFormat.outDirtyRecords.inc();
            }
        });
        rows.clear();
    }

    private void putRowIntoMap(Map<String, ArrayList<String>> rowDataMap, Tuple2<String, String> rowData) {
        Set<String> keySet = rowDataMap.keySet();
        ArrayList<String> tempRowArray;
        if (keySet.contains(rowData.f0)) {
            tempRowArray = rowDataMap.get(rowData.f0);
        } else {
            tempRowArray = new ArrayList<>();
        }
        tempRowArray.add(rowData.f1);
        rowDataMap.put(rowData.f0, tempRowArray);
    }

    private List<String> rebuildFieldNameListAndTypeList(List<String> fieldNames, List<String> staticPartitionFields, List<String> fieldTypes, String partitionFields) {
        if (partitionFields == null || partitionFields.isEmpty()) {
            return fieldNames;
        }

        List<String> valueFields = new ArrayList<>(fieldNames);

        for (int i = valueFields.size() - 1; i >= 0; i--) {
//            System.out.println(staticPartitionFields+",,"+fieldNames.get(i));
            if (staticPartitionFields.contains(fieldNames.get(i))) {
                valueFields.remove(i);
                fieldTypes.remove(i);
            }
        }

        for (int i = 0; i < valueFields.size(); i++) {
            if (partitionFields.contains(fieldNames.get(i))) {
                valueFields.add(valueFields.remove(i));
                fieldTypes.add(fieldTypes.remove(i));
            }
        }

        return valueFields;
    }

    @Override
    public void writeRecord(IN record) throws IOException {
        try {
            Row row = (Row) converter.toExternal(record);
            if (row==null) {
                return;
            }

            if (outRecords.getCount() % RECEIVE_DATA_PRINT_FREQUENCY == 0 || LOG.isDebugEnabled()) {
                LOG.info("Receive data : {}", record);
            }

            if (updateMode.equalsIgnoreCase(UPDATE_MODE)) {
                rows.add(Row.copy(row));
            } else {
                Map<String, Object> valueMap = Maps.newHashMap();
//                Row row = Row.copy(record.f1);

                for (int i = 0; i < row.getArity(); i++) {
                    valueMap.put(fieldNames.get(i), row.getField(i));
                }

                Tuple2<String, String> rowTuple2 = new Tuple2<>();
                if (storeType.equalsIgnoreCase(KUDU_TYPE) || !enablePartition) {
                    rowTuple2.f0 = NO_PARTITION;
                } else {
                    rowTuple2.f0 = buildPartitionCondition(valueMap, partitionFields, staticPartitionFields);
                }

                // ?????????????????? row data ??????, ??????????????? row data : (1, xxx, 20) -> (id, name, age)
                // ???????????? partition????????????field ??????????????? (name, id, age)??????????????? row data ???????????? (xxx, 1, 20)
                Row rowValue = new Row(fieldTypes.size());
                for (int i = 0; i < fieldTypes.size(); i++) {
                    rowValue.setField(i, valueMap.get(valueFieldNames.get(i)));
                }
                rowTuple2.f1 = buildValuesCondition(fieldTypes, rowValue);
                putRowIntoMap(rowDataMap, rowTuple2);
            }

            batchCount++;

            if (batchCount >= batchSize) {
                flush();
            }

            // Receive data
            outRecords.inc();
        } catch (Exception e) {
            throw new IOException("Writing records to impala failed.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        // ??????????????????SQL flush
        if (batchCount > 0) {
            flush();
        }
        // cancel scheduled task
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
        // close connection
        try {
            if (connection != null && connection.isValid(DEFAULT_CONN_TIME_OUT)) {
                connection.close();
            }

            if (statement != null && !statement.isClosed()) {
                statement.close();
            }

            if (updateStatement != null && !updateStatement.isClosed()) {
                updateStatement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("impala connection close failed!", e);
        } finally {
            connection = null;
            statement = null;
            updateStatement = null;
        }
        closed = true;
    }

    /**
     * execute batch sql from row data map
     * sql like 'insert into tableName(f1, f2, f3) ${partitionCondition} values(v1, v2, v3), (v4, v5, v6)....
     *
     * @param tempSql         template sql
     * @param storeType       the store type of data
     * @param enablePartition enable partition or not
     * @param fieldNames      field name list
     * @param partitionFields partition fields
     * @param rowDataMap      row data map
     */
    private void executeBatchSql(String tempSql,
                                 String schema,
                                 String tableName,
                                 String storeType,
                                 Boolean enablePartition,
                                 List<String> fieldNames,
                                 String partitionFields,
                                 Map<String, ArrayList<String>> rowDataMap) {
        StringBuilder partitionCondition = new StringBuilder();
        String tableFieldsCondition = buildTableFieldsCondition(fieldNames, partitionFields);
        ArrayList<String> rowData = new ArrayList<>();
        String tableNameInfo = Objects.isNull(schema) ?
                tableName : quoteIdentifier(schema) + "." + tableName;
        tempSql = tempSql.replace("tableName", tableNameInfo);
        boolean isPartitioned = storeType.equalsIgnoreCase(KUDU_TYPE) || !enablePartition;

        try {
            // kudu ${partitionCondition} is null
            if (isPartitioned) {
                tempSql = tempSql
                    .replace(PARTITION_CONDITION, partitionCondition.toString())
                    .replace(PARTITION_CONSTANT, "")
                    .replace(TABLE_FIELDS_CONDITION, tableFieldsCondition);
                rowData.addAll(rowDataMap.get(NO_PARTITION));
                String executeSql = tempSql.replace(VALUES_CONDITION, String.join(", ", rowData));
                statement.execute(executeSql);
                rowData.clear();
            } else {
                // partition sql
                Set<String> keySet = rowDataMap.keySet();
                for (String key : keySet) {
                    rowData.addAll(rowDataMap.get(key));
                    partitionCondition.append(key);
                    tempSql = tempSql
                        .replace(
                                PARTITION_CONDITION,
                                partitionCondition.toString()
                                        .replaceAll("\\$\\{", "")
                                        .replaceAll("}","")
                        )
                        .replace(TABLE_FIELDS_CONDITION, tableFieldsCondition);
                    String executeSql = tempSql
                        .replace(VALUES_CONDITION, String.join(", ", rowData));
                    statement.execute(executeSql);
                    partitionCondition.delete(0, partitionCondition.length());
                }
            }
        } catch (Exception e) {
            if (e instanceof SQLException) {
                dealBatchSqlError(rowData, connection, statement, tempSql);
            } else {
                throw new RuntimeException("Insert into impala error!", e);
            }
        } finally {
            rowData.clear();
        }
    }

    /**
     * ???????????????????????????????????????sql???????????????sql??????????????????????????????sql???????????????
     *
     * @param rowData ?????????values
     * @param connection ???????????????connect
     * @param statement ??????statement
     * @param templateSql ??????sql?????????insert into tableName(f1, f2, f3) [partition] values $valueCondition
     */
    private void dealBatchSqlError(List<String> rowData,
                                   Connection connection,
                                   Statement statement,
                                   String templateSql) {
        String errorMsg = "Insert into impala error. \nCause: [%s]\nRow: [%s]";
        JdbcConnectionUtil.rollBack(connection);
        JdbcConnectionUtil.commit(connection);
        for (String rowDatum : rowData) {
            String executeSql = templateSql.replace(VALUES_CONDITION, rowDatum);
            try {
                statement.execute(executeSql);
                JdbcConnectionUtil.commit(connection);
            } catch (SQLException e) {
                JdbcConnectionUtil.rollBack(connection);
                JdbcConnectionUtil.commit(connection);
                if (metricOutputFormat.outDirtyRecords.getCount() % DIRTY_DATA_PRINT_FREQUENCY == 0 || LOG.isDebugEnabled()) {
                    LOG.error(
                        String.format(
                            errorMsg,
                            e.getMessage(),
                            rowDatum)
                    );
                }
                metricOutputFormat.outDirtyRecords.inc();
            }
        }
    }

    /**
     * build partition condition with row data
     *
     * @param rowData              row data
     * @param partitionFields      partition fields
     * @param staticPartitionField static partition fields
     * @return condition like '(ptOne, ptTwo=v2)'
     */
    private String buildPartitionCondition(Map<String, Object> rowData, String partitionFields, List<String> staticPartitionField) {
        for (String key : staticPartitionField) {
            StringBuilder sb = new StringBuilder();
            Object value = rowData.get(key);
            sb.append(key).append("=").append(value);
            partitionFields = partitionFields.replace("${" + key + "}", sb.toString());
        }
        return "(" + partitionFields + ")";
    }

    /**
     * build field condition according to field names
     * replace ${tableFieldCondition}
     *
     * @param fieldNames      the selected field names
     * @param partitionFields the partition fields
     * @return condition like '(id, name, age)'
     */
    private String buildTableFieldsCondition(List<String> fieldNames, String partitionFields) {
        return "(" + fieldNames.stream()
                .filter(f -> !partitionFields.contains(f))
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", ")) + ")";
    }

    /**
     * according to field types, build the values condition
     * replace ${valuesCondition}
     *
     * @param fieldTypes field types
     * @return condition like '(?, ?, cast('?' as string))' and '?' will be replaced with row data
     */
    private String buildValuesCondition(List<String> fieldTypes, Row row) {
        String valuesCondition = fieldTypes.stream().map(
                f -> {
                    for (String item : NEED_QUOTE_TYPE) {
                        if (f.toLowerCase().contains(item)) {
                            return String.format("cast('?' as %s)", f.toLowerCase());
                        }
                    }
                    return "?";
                }).collect(Collectors.joining(", "));
        for (int i = 0; i < row.getArity(); i++) {
            Object rowField = row.getField(i);
            if (DtStringUtil.isEmptyOrNull(rowField)) {
                valuesCondition = valuesCondition.replaceFirst("'\\?'", "null");
            } else {
                valuesCondition = valuesCondition.replaceFirst("\\?", Matcher.quoteReplacement(rowField.toString()));
            }
        }
        return "(" + valuesCondition + ")";
    }

    /**
     * impala update mode SQL
     *
     * @return UPDATE tableName SET setCondition WHERE whereCondition
     */
    private String buildUpdateSql(String schema, String tableName, List<String> fieldNames, List<String> primaryKeys) {
        //??????primary key??????
        String setClause = fieldNames.stream()
                .filter(f -> !CollectionUtils.isNotEmpty(primaryKeys) || !primaryKeys.contains(f))
                .map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(", "));

        String conditionClause = primaryKeys.stream()
                .map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(" AND "));

        return "UPDATE " + (Objects.isNull(schema) ? "" : quoteIdentifier(schema) + ".")
                + quoteIdentifier(tableName) + " SET " + setClause + " WHERE " + conditionClause;
    }

    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public static class Builder {
        private final ImpalaOutputFormat format = new ImpalaOutputFormat();

        public Builder setDbUrl(String dbUrl) {
            format.dbUrl = dbUrl;
            return this;
        }
        public Builder setConverter(DynamicTableSink.DataStructureConverter converter) {
            format.converter = converter;
            return this;
        }

        public Builder setUserName(String userName) {
            format.userName = userName;
            return this;
        }

        public Builder setPassword(String password) {
            format.password = password;
            return this;
        }

        public Builder setBatchSize(Integer batchSize) {
            format.batchSize = batchSize;
            return this;
        }

        public Builder setBatchWaitInterval(Long batchWaitInterval) {
            format.batchWaitInterval = batchWaitInterval;
            return this;
        }

        public Builder setTableName(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public Builder setPartitionFields(String partitionFields) {
            format.partitionFields = Objects.isNull(partitionFields) ?
                    "" : partitionFields;
            return this;
        }

        public Builder setPrimaryKeys(List<String> primaryKeys) {
            format.primaryKeys = primaryKeys;
            return this;
        }

        public Builder setSchema(String schema) {
            format.schema = schema;
            return this;
        }

        public Builder setEnablePartition(Boolean enablePartition) {
            format.enablePartition = enablePartition;
            return this;
        }

        public Builder setUpdateMode(String updateMode) {
            format.updateMode = updateMode;
            return this;
        }

        public Builder setFieldList(List<String> fieldList) {
            format.fieldNames = fieldList;
            return this;
        }

        public Builder setFieldTypeList(List<String> fieldTypeList) {
            format.fieldTypes = fieldTypeList;
            return this;
        }

        public Builder setStoreType(String storeType) {
            format.storeType = storeType;
            return this;
        }

//        public Builder setFieldExtraInfoList(List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList) {
//            format.fieldExtraInfoList = fieldExtraInfoList;
//            return this;
//        }

        public Builder setKeyTabPath(String keyTabPath) {
            format.keytabPath = keyTabPath;
            return this;
        }

        public Builder setKrb5ConfPath(String krb5ConfPath) {
            format.krb5confPath = krb5ConfPath;
            return this;
        }

        public Builder setPrincipal(String principal) {
            format.principal = principal;
            return this;
        }

        public Builder setAuthMech(Integer authMech) {
            format.authMech = authMech;
            return this;
        }

        private boolean canHandle(String url) {
            return url.startsWith("jdbc:impala:");
        }

        public ImpalaOutputFormat build() {
            if (!canHandle(format.dbUrl)) {
                throw new IllegalArgumentException("impala dbUrl is illegal, check url: " + format.dbUrl);
            }

//            if (format.authMech == EAuthMech.Kerberos.getType()) {
//                checkNotNull(format.krb5confPath,
//                        "When kerberos authentication is enabled, krb5confPath is required???");
//                checkNotNull(format.principal,
//                        "When kerberos authentication is enabled, principal is required???");
//                checkNotNull(format.keytabPath,
//                        "When kerberos authentication is enabled, keytabPath is required???");
//            }
//
//            if (format.authMech == EAuthMech.UserName.getType()) {
//                checkNotNull(format.userName, "userName is required!");
//            }
//
//            if (format.authMech == EAuthMech.NameANDPassword.getType()) {
//                checkNotNull(format.userName, "userName is required!");
//                checkNotNull(format.password, "password is required!");
//            }
//
//            checkNotNull(format.storeType, "storeType is required!");

            return format;
        }
    }
}
