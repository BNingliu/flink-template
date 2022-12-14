package org.apache.flink.streaming.connectors.greenplum.table;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.greenplum.data.BuildData;
import org.apache.flink.streaming.connectors.greenplum.data.CopyWorker;
import org.apache.flink.streaming.connectors.greenplum.util.DBUtil;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class GreenPlumSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(GreenPlumSinkFunction.class);

    private final JdbcConnectorOptions jdbcOptions;
    //    private DataType dateType;
//    private List<String> pkColumns;
    private List<String> columns;
    private Integer maxSize;

    public GreenPlumSinkFunction(
            JdbcConnectorOptions jdbcOptions,
            DataType dataType,
            ResolvedCatalogTable catalogTable,
            Integer maxSize
    ) {
        this.jdbcOptions = jdbcOptions;
//        this.dateType = dataType;
//        this.pkColumns = catalogTable.getResolvedSchema().getPrimaryKey().get().getColumns();
        this.columns = catalogTable.getResolvedSchema().getColumns().stream().map(Column::getName).collect(Collectors.toList());
        this.maxSize = maxSize;
    }


    private Connection connection = null;
    protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;
    List<byte[]> dataList = new ArrayList<>();


    String sql = null;
    BuildData readDataProcessor = null;
    CopyWorker dataCopyWorker = null;
    LinkedBlockingQueue<byte[]>   dataQueue = null;


    @Override
    public void open(Configuration parameters) {
        try {
            connection = DBUtil.getConnection(
                    jdbcOptions.getDbURL(),
                    jdbcOptions.getUsername().get(),
                    jdbcOptions.getPassword().get(),
                    jdbcOptions.getDriverName()
            );
            sql = DBUtil.getCopySql(jdbcOptions.getTableName(), columns, 0);
            this.resultSetMetaData
                    = DBUtil.getColumnMetaData(connection, jdbcOptions.getTableName(), DBUtil.constructColumnNameList(columns));
            readDataProcessor = new BuildData(columns.size(), resultSetMetaData);
            dataCopyWorker = new CopyWorker(connection, sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void invoke(IN value, Context context) throws Exception {
        try {
            RowData rowData = (RowData) value;
            byte[] bytes = readDataProcessor.serializeRecord(rowData);
            dataList.add(bytes);
            if (dataList.size() >= maxSize) {
                dataCopyWorker.copyData(dataList, jdbcOptions);
                dataList.clear();
            }

        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage());
            throw new FlinkRuntimeException("invoke error",e);
        }

    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("data size: {}", dataList.size());
        dataCopyWorker.copyData(dataList, jdbcOptions);
        dataList.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("");
    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("");
    }


}
