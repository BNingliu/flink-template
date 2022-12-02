package org.apache.flink.streaming.connectors.impala.table;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @program: tms-flink
 * @description:
 * @author: liuningbo
 * @create: 2022/05/31 16:05SupportsReadingMetadata,SupportsPartitioning, SupportsOverwrite
 */
public class ImpalaSink implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

    private final String host;
    private final String username;
    private final String password;
    private final String database;
    private final String tableName;
    private final ResolvedSchema resolvedSchema;
    private final String storeType;
    private final String partitionFields;
    private DataStructureConverter converter;
    private final Integer batchSize;
    private final Integer authMech;
    private final Integer parallelism;

    public ImpalaSink(
            String host
            , String username
            , String password
            , String database
            , String tableName
            , ResolvedSchema resolvedSchema
            , String storeType
            , String partitionFields
            ,Integer batchSize
            ,Integer authMech
            ,Integer parallelism
    ) {
        this.host = host;
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.resolvedSchema = resolvedSchema;
        this.storeType = storeType;
        this.partitionFields=partitionFields;
        this.batchSize=batchSize;
        this.authMech=authMech;
        this.parallelism= parallelism;

    }

    //ChangelogMode
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    //具体运行的地方，真正开始调用用户自己定义的 streaming sink ，建立 sql 与 streaming 的联系
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        converter = context.createDataStructureConverter(resolvedSchema.toPhysicalRowDataType());
        return (DataStreamSinkProvider)
                dataStream -> consume(dataStream, context.isBounded());
    }

    private DataStreamSink<?> consume(
            DataStream<RowData> dataStream
            , boolean isBounded
    ) {
        ImpalaOutputFormat outputFormat = buildImpalaOutputFormat();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(outputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction)
                .setParallelism(Optional.ofNullable(parallelism).orElse(1))
                .name(tableName);
        return dataStreamSink;
    }


    private ImpalaOutputFormat buildImpalaOutputFormat() {

        List<String> fieldList = resolvedSchema.getColumnNames();
        List<String> fieldTypeList = resolvedSchema.getColumnDataTypes().stream()
                .map(m -> m.toString()).collect(Collectors.toList());

        long batchWaitInterval = 60 * 1000L;

        return ImpalaOutputFormat.getImpalaBuilder()
                .setDbUrl(host)
                .setPassword(password)
                .setUserName(username)
                .setSchema(database)
                .setTableName(tableName)
                .setConverter(converter)
                .setBatchSize(batchSize)
                .setBatchWaitInterval(batchWaitInterval)
                .setPartitionFields(partitionFields)
                .setFieldList(fieldList)
                .setFieldTypeList(fieldTypeList)
                .setStoreType(storeType)
                .setEnablePartition(true)
                .setUpdateMode("APPEND")
                .setAuthMech(authMech)
                //.setPrimaryKeys(primaryKeys)
//                .setKeyTabPath(keytabPath)
//                .setKrb5ConfPath(krb5confPath)
//                .setPrincipal(principal)
                .build();
    }


    // sink 可以不用实现，主要用来 source 的谓词下推
    @Override
    public DynamicTableSink copy() {
        ImpalaSink impalaSink = new ImpalaSink(
                host,
                username,
                password,
                database,
                tableName,
                resolvedSchema,
                storeType,
                partitionFields,
                batchSize,
                authMech,
                parallelism
        );
        return impalaSink;
    }

    @Override
    public String asSummaryString() {
        return "impalaSink";
    }

    @Override
    public void applyOverwrite(boolean b) {
        System.out.println(b);
    }

    @Override
    public void applyStaticPartition(Map<String, String> map) {
        System.out.println(map);
    }
}
