package com.ytd.template.api;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * mysql到mysql
 */
public class MysqlToClickHouseSqlDemo {

    public static void main(String[] args) throws Exception {
//1.创建执行环境

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.disableOperatorChaining();
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
                "  distinct_id  bigint ," +
                "  tag_value STRING,\n" +
                "    base_day STRING,\n" +
                "    g_key STRING,\n" +
                "    tag_code STRING,\n" +
                "  PRIMARY KEY (distinct_id) NOT ENFORCED " +
                ") WITH (\n" +
                "    'connector' = 'clickhouse' ,\n" +
                "    'url' = 'clickhouse://192.168.2.131:8123',\n" +
                "    'database-name' = 'default',\n" +
                "    'table-name' = 'portrait',\n" +
                "     'sink.batch-size' = '500',\n" +
                "   'sink.flush-interval' = '1000', \n" +
                "   'sink.max-retries' = '3', \n" +
                "    'username' = 'default',\n" +
                "    'password' = '123456' \n" +
                ")");

        TableResult tableResult = tableEnv.executeSql(
                "INSERT INTO pvuv_sink SELECT\n" +
                        " id , " +
                        " '1'  as tag_value ,\n" +
                        " '2022-12-08' as base_day ,\n" +
                        "  name as g_key, \n" +
                        " cast (tenant_id as string)  as tag_code  \n" +
                        "FROM ytd_user_group ");
        tableResult.print();

        env.execute();
    }

}
