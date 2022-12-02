package com.ytd.template.api;


import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Optional;

public class FlinkCdcSqlTest {

    public static void main(String[] args) throws Exception {
//1.创建执行环境
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();
                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//2.创建 Flink-MySQL-CDC 的 Source
//        tableEnv.executeSql("create table ytd_user_group (" +
//                "    id           bigint primary key,\n" +
//                "    created_time TIMESTAMP(6),\n" +
//                "    desc         STRING,\n" +
//                "    name         STRING,\n" +
//                "    tenant_id    int,\n" +
//                "    app_id       int " +
//                ")with(" +
//                "      'connector' = 'mysql-cdc',\n" +
//                "      'hostname' = 'rm-uf65iwa3laojof99n6o.mysql.rds.aliyuncs.com',\n" +
//                "      'port' = '3306',\n" +
//                "      'username' = 'root',\n" +
//                "      'password' = '123qweQWE',\n" +
//                "      'database-name' = 'data_integration',\n" +
//                "      'scan.startup.mode' = 'initial',\n" +
//                "      'table-name' = 'ytd_user_group' " +
//                " )");

        tableEnv.executeSql("CREATE TABLE ytd_user_group (\n" +
                "    id           bigint primary key,\n" +
                "    created_time TIMESTAMP(6),\n" +
                "    desc         STRING,\n" +
                "    name         STRING,\n" +
                "    tenant_id    int,\n" +
                "    app_id       int\n" +
                ") WITH (\n" +
                "      'connector' = 'datagen'\n" +
                "      )");


//        tableEnv.executeSql("create table pvuv_sink  (" +
//                "    tenant_id BIGINT primary key ,\n" +
//                "    dt VARCHAR,\n" +
//                "    pv BIGINT,\n" +
//                "    uv BIGINT " +
//                ")with(" +
//                " 'connector.type' = 'jdbc',\n" +
//                "    'connector.url' = 'jdbc:mysql://rm-uf65iwa3laojof99n6o.mysql.rds.aliyuncs.com:3306/streamx',\n" +
//                "    'connector.table' = 'pvuv_sink',\n" +
//                "    'connector.username' = 'root',\n" +
//                "    'connector.password' = '123qweQWE',\n" +
//                "    'connector.write.flush.max-rows' = '1'" +
//                " )");
//
//        TableResult result = tableEnv.executeSql("SELECT\n" +
//                " tenant_id,\n" +
//                "  DATE_FORMAT(created_time, 'yyyy-MM-dd HH:00') dt,\n" +
//                "  COUNT(*) AS pv,\n" +
//                "  COUNT(DISTINCT tenant_id) AS uv\n" +
//                "FROM ytd_user_group\n" +
//                "GROUP BY DATE_FORMAT(created_time, 'yyyy-MM-dd HH:00'),tenant_id");
        TableResult result = tableEnv.executeSql(
                "SELECT\n" +
                " tenant_id,\n" +
                "  DATE_FORMAT(created_time, 'yyyy-MM-dd HH:00') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT tenant_id) AS uv\n" +
                "FROM ytd_user_group\n" +
                "GROUP BY DATE_FORMAT(created_time, 'yyyy-MM-dd HH:00'),tenant_id");
        try {
            result.print();
            Optional<JobClient> jobClient = result.getJobClient();
            JobID jobID = jobClient.get().getJobID();
            System.out.println(jobID.toString()+"成功了");
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
