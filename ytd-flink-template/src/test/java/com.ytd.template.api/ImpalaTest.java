package com.ytd.template.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ImpalaTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //创建mysql cdc 数据源
        env.setParallelism(1);
        env.enableCheckpointing(60000);  //头和头

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
                "      'hostname' = 'rm-uf65iwa3laojof99n6o.mysql.rds.aliyuncs.com',\n" +
                "      'port' = '3306',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123qweQWE',\n" +
                "      'database-name' = 'data_integration',\n" +
                "      'scan.startup.mode' = 'initial',\n" +
                "      'table-name' = 'ytd_user_group'\n" +
                "      )");
//        tableEnv.executeSql("CREATE TABLE ytd_user_group (\n" +
//                "    id           bigint primary key,\n" +
//                "    created_time TIMESTAMP(6),\n" +
//                "    desc         STRING,\n" +
//                "    name         STRING,\n" +
//                "    tenant_id    int,\n" +
//                "    app_id       int\n" +
//                ") WITH (\n" +
//                "      'connector' = 'datagen'\n" +
//                "      )");

        tableEnv.executeSql("create table pvuv_sink (" +
                "  tenant_id  bigint ," +
                "  dt VARCHAR,\n" +
                "    pv BIGINT,\n" +
                "    uv BIGINT\n" +
                ")   with(" +
                "'connector' = 'impala'," +
                "'host' = 'jdbc:impala://192.168.2.131:21050'," +
                "'auth-mech' = '3'," +
                "'username' = 'impala'," +
                "'password' = 'impala'," +
                "'store-type' = 'PARQUET'," +
                " 'parallelism' = '2',  "+
                "'partition-fields' = 'tenant_id,${pv=2}' ," +
                " 'batchSize' = '100', " +
                "'database-name' = 'default'," +
                "'table-name' = 'pvuv_sink2'" +
                ")");
//        partition (part_init_date='20190425',interval_type=1)
        TableResult tableResult = tableEnv.executeSql(
                " insert into pvuv_sink SELECT\n" +
                        " tenant_id, " +
                        " DATE_FORMAT(created_time, 'yyyy-MM-dd') dt,\n" +
                        "  COUNT(*) AS pv,\n" +
                        "  COUNT(DISTINCT tenant_id) AS uv\n" +
                        "FROM ytd_user_group  where tenant_id>0 \n" +
                        "GROUP BY DATE_FORMAT(created_time, 'yyyy-MM-dd'),tenant_id");

        tableResult.print();
        env.execute();

    }
}

