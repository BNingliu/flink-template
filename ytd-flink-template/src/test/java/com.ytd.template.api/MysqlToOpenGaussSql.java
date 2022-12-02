package com.ytd.template.api;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * mysql到mysql
 */
public class MysqlToOpenGaussSql {

    public static void main(String[] args) throws Exception {
//1.创建执行环境
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("create table ytd_user_group\n" +
                "(\n" +
                "    id           bigint primary key,\n" +
                "    created_time TIMESTAMP(6),\n" +
                "    desc         STRING,\n" +
                "    name         STRING,\n" +
                "    tenant_id    int,\n" +
                "    app_id       int\n" +
                ") with (\n" +
                "      'connector' = 'mysql-cdc',\n" +
                "      'hostname' = '192.168.2.131',\n" +
                "      'port' = '3306',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'ytdinfo123',\n" +
                "      'database-name' = 'test',\n" +
                "      'scan.startup.mode' = 'initial',\n" +
                "      'table-name' = 'ytd_user_group'\n" +
                "      )");

        tableEnv.executeSql("CREATE TABLE pvuv_sink (\n" +
                "  tenant_id  INT ," +
                "  dt VARCHAR,\n" +
                "    pv BIGINT,\n" +
                "    uv BIGINT  \n" +
                "   " +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:postgresql://192.168.2.131:5434/test_andy?currentSchema=public&reWriteBatchedInserts=false',\n" +
                "    'connector.table' = 'pvuv_sink',\n" +
                "    'connector.username' = 'gaussdb',\n" +
                "    'connector.password' = 'Enmo@123',\n" +
                "    'connector.write.flush.max-rows' = '1'\n" +
                ")");

        TableResult tableResult = tableEnv.executeSql(
                "INSERT INTO pvuv_sink SELECT\n" +
                        " tenant_id, " +
                        " DATE_FORMAT(created_time, 'yyyy-MM-dd') dt,\n" +
                        "  COUNT(*) AS pv,\n" +
                        "  COUNT(DISTINCT tenant_id) AS uv\n" +
                        "FROM ytd_user_group\n" +
                        "GROUP BY DATE_FORMAT(created_time, 'yyyy-MM-dd'),tenant_id");
        tableResult.print();

//        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
//
//        retractStream.print();

        env.execute();
    }

}
