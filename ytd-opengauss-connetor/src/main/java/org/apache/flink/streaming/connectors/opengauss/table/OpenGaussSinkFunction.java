package org.apache.flink.streaming.connectors.opengauss.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

/**
 * @author lijiaqi
 */
public class OpenGaussSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String MAX_PARALLEL_REPLICAS_VALUE = "2";

    private final JdbcConnectorOptions jdbcOptions;
    private final SerializationSchema<RowData> serializationSchema = null;
    private DataType dateType;
    private Connection conn;
    private Statement stmt;

    private List<String>  pkColumns;

    public OpenGaussSinkFunction(JdbcConnectorOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
//        this.serializationSchema = serializationSchema;
    }

    public OpenGaussSinkFunction(JdbcConnectorOptions jdbcOptions, DataType dataType, ResolvedCatalogTable catalogTable) {
        this.jdbcOptions = jdbcOptions;
        this.dateType = dataType;
        this.pkColumns = catalogTable.getResolvedSchema().getPrimaryKey().get().getColumns();
    }

    @Override
    public void open(Configuration parameters) {
        System.out.println("open connection !!!!!");
        try {
            if (null == conn) {
                Class.forName(jdbcOptions.getDriverName());
                conn = DriverManager.getConnection(
                        jdbcOptions.getDbURL()
                        , jdbcOptions.getUsername().orElse(null)
                        , jdbcOptions.getPassword().orElse(null));
//                conn = dataSource.getConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {

//        System.out.println("into ....");
        try {

//            insert into pvuv_sink (tenant_id, dt, pv, uv)
//            values (4, '2019-05-24', 1, 1) ,(3, '2019-05-24', 1, 1)
//            ON DUPLICATE KEY
//                    update
//            dt=excluded.dt,
//                    pv=excluded.pv,
//                    uv=excluded.uv
//                    ;
            stmt = conn.createStatement();
            String sql = "insert into " + this.jdbcOptions.getTableName() + " values ( ";
            for (int i = 0; i < value.getArity(); i++) {
                Class<?> conversionClass = dateType.getChildren().get(i).getConversionClass();
                if (conversionClass.equals(Integer.class)) {
                    sql += +value.getInt(i) + " ,";
                } else if (conversionClass.equals(Long.class)) {
                    sql += +value.getLong(i) + " ,";
                } else {
                    sql += "'" + value.getString(i) + "' ,";
                }


            }
            RowType logicalType = (RowType) dateType.getLogicalType();
            List<String> fieldNames = logicalType.getFieldNames();


            sql = sql.substring(0, sql.length() - 1);
            sql += " ) ";
            sql +=" ON DUPLICATE KEY update  ";
            for (String fieldName : fieldNames) {
                if(pkColumns.contains(fieldName)){
                    continue;
                }
                sql += fieldName+"=excluded."+fieldName +",";
            }
            sql = sql.substring(0, sql.length() - 1);
            sql +=";";
            System.out.println("sql ==>" + sql);

            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        stmt.write().table(jdbcOptions.getTableName()).data(new ByteArrayInputStream(serialize), ClickHouseFormat.JSONEachRow)
//                .addDbParam(ClickHouseQueryParam.MAX_PARALLEL_REPLICAS, MAX_PARALLEL_REPLICAS_VALUE).send();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
