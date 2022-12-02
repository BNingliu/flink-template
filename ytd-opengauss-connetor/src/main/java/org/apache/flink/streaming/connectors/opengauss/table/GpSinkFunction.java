package org.apache.flink.streaming.connectors.opengauss.table;

import com.huawei.shade.com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.opengauss.gp.CopyWorker;
import org.apache.flink.streaming.connectors.opengauss.gp.GpWriter;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author lijiaqi
 */
public class GpSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, CheckpointListener {
    private final JdbcConnectorOptions jdbcOptions;
    private List<String>  pkColumns;
    private DataType dateType;

    private transient GpWriter gpWriter;

//    private Connection conn;
//    private Statement stmt;
//    private final SerializationSchema<RowData> serializationSchema = null;
        private CompletionService<Long> cs = null;

    public GpSinkFunction(JdbcConnectorOptions jdbcOptions, DataType dataType, ResolvedCatalogTable catalogTable) {
        this.jdbcOptions = jdbcOptions;
        this.dateType = dataType;
        this.pkColumns = catalogTable.getResolvedSchema().getPrimaryKey().get().getColumns();
    }

    @Override
    public void open(Configuration parameters) {
        try {
            gpWriter = new GpWriter(jdbcOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void invoke(IN value, Context context) throws Exception {
        String sql = getCopySql("test",this.pkColumns, 0);
        LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<byte[]>(10);
//        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        RowData rowData = (RowData) value;
        int arity = rowData.getArity();

        for (int i = 0; i < arity; i++) {
            Class<?> conversionClass = dateType.getChildren().get(i).getConversionClass();
            if (conversionClass.equals(Integer.class)) {
                sql += +rowData.getInt(i) + " ,";
            } else if (conversionClass.equals(Long.class)) {
                sql += +rowData.getLong(i) + " ,";
            } else {
                sql += "'" + rowData.getString(i) + "' ,";
            }

        }

        dataQueue.add(JSON.toJSONString(rowData).getBytes("UTF-8"));
//
//        ExecutorService threadPool;
//        Integer numWriter =1;
//        Integer numProcessor = 4;
//        threadPool = Executors.newFixedThreadPool(4);

        try {
            if(dataQueue.size()>=3){
                new CopyWorker( sql, dataQueue);
            }
            dataQueue.clear();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            threadPool.shutdownNow();
        }
    }
//    @Override
//    public void invoke(IN value, Context context) throws Exception {
//        String sql = getCopySql("test",this.pkColumns, 0);
//        LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<byte[]>(10);
//
//        RowData rowData = (RowData) value;
//        LinkedBlockingQueue<RowData> recordQueue = new LinkedBlockingQueue<RowData>(10);
//
//        ExecutorService threadPool;
//         Integer numWriter =1;
//         Integer numProcessor = 4;
//        threadPool = Executors.newFixedThreadPool(4);
//        cs = new ExecutorCompletionService<Long>(threadPool);
//
//        try {
//            for (int i = 0; i < numWriter; i++) {
//                cs.submit(new CopyWorker( sql, dataQueue));
//            }
//
////            RowData record;
////            while ((record = recordReceiver.getFromReader()) != null) {
////                send(record, recordQueue);
////                Future<Long> result = cs.poll();
////
////                if (result != null) {
////                    result.get();
////                }
////            }
//
//            for (int i = 0; i < numWriter; i++) {
//                cs.take().get();
//            }
//        } catch (Exception e) {
//          e.printStackTrace();
//        } finally {
//            threadPool.shutdownNow();
//        }
//    }
    private void send(RowData record, LinkedBlockingQueue<RowData> queue)
            throws InterruptedException, ExecutionException {
        while (queue.offer(record, 1000, TimeUnit.MILLISECONDS) == false) {
            Future<Long> result = cs.poll();

            if (result != null) {
                result.get();
            }
        }
    }
    public String getCopySql(String tableName, List<String> columnList, int segment_reject_limit) {
        StringBuilder sb = new StringBuilder().append("COPY ").append(tableName).append("(")
                .append(constructColumnNameList(columnList))
                .append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");
        if (segment_reject_limit >= 2) {
            sb.append(" LOG ERRORS SEGMENT REJECT LIMIT ").append(segment_reject_limit).append(";");
        } else {
            sb.append(";");
        }

        String sql = sb.toString();
        return sql;
    }

    private String constructColumnNameList(List<String> columnList) {
        List<String> columns = new ArrayList<String>();

        for (String column : columnList) {
            if (column.endsWith("\"") && column.startsWith("\"")) {
                columns.add(column);
            } else {
                columns.add("\"" + column + "\"");
            }
        }

        return StringUtils.join(columns, ",");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {


    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }


}
