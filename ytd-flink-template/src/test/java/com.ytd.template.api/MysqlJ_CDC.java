package com.ytd.template.api;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * mysql到mysql
 */
public class MysqlJ_CDC {

    public static void main(String[] args) throws Exception {
//1.创建执行环境
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);  //头和头

        env.disableOperatorChaining();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("create table ytd_user_group\n" +
                "(\n" +
                "    id           bigint primary key,\n" +
//                "    created_time TIMESTAMP(6),\n" +
//                "    desc         STRING,\n" +
//                "    name         STRING,\n" +
//                "    tenant_id    int,\n" +
//                "    app_id       int\n" +
                "    log_path       STRING\n" +

                ") with (\n" +
                "      'connector' = 'mysql-cdc',\n" +
                "      'hostname' = 'rm-uf65iwa3laojof99n6o.mysql.rds.aliyuncs.com',\n" +
                "      'port' = '3306',\n" +
//                "      'scan.incremental.snapshot.enabled' = 'false',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123qweQWE',\n" +
                "      'database-name' = 'data_integration',\n" +
                "      'scan.startup.mode' = 'initial',\n" +
//                "      'heartbeat.interval' = '0s',\n" +
                "      'table-name' = 'di_data_flow_history'\n" +
                "      )");

        Table table = tableEnv.sqlQuery("select id,log_path from ytd_user_group");

//        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
//        rowDataStream.map(new RichMapFunction<Row, Row>() {
//            private transient Counter counter;
//            @Override
//            public void open(Configuration config) {
//                counter = getRuntimeContext()
//                        .getMetricGroup()
//                        .counter("myCounter");
//            }
//
//            @Override
//            public Row map(Row value) throws Exception {
//                this.counter.inc();
//                return value;
//            }
//
//        });

        tableEnv.executeSql("CREATE TABLE test_sink (\n" +
                "                             id bigint,\n" +
                "                             log_path STRING\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://rm-uf65iwa3laojof99n6o.mysql.rds.aliyuncs.com:3306/flink_web',\n" +
                "    'connector.table' = 'test_sink',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = '123qweQWE',\n" +
                "    'connector.write.flush.max-rows' = '1'\n" +
                ")");

//        tableEnv.executeSql("CREATE TABLE test_sink (\n" +
//                "                             id bigint,\n" +
//                "                             log_path STRING\n" +
//                ") WITH (\n" +
//                "      'connector' = 'print'\n" +
//                "      )");

        table.executeInsert("test_sink");
//        TableResult tableResult = tableEnv.executeSql(
//                "INSERT INTO test_sink " +
//                        "SELECT  id,log_path FROM ytd_user_group group by id,log_path" );
//        tableResult.print();

//        table.wait();
        System.out.println("结束");
//        System.exit(1);
//        env.execute();
    }

}
