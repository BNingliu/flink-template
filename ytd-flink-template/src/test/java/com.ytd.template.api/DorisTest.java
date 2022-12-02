package com.ytd.template.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;
import java.util.Properties;

public class DorisTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //创建mysql cdc 数据源
        env.setParallelism(1);
        env.enableCheckpointing(60000);  //头和头

        tableEnv.executeSql("CREATE TABLE p8_source (" +
                " `tag_code` VARCHAR(250) ,\n" +
                "  `distinct_id` VARCHAR(250)  ,\n" +
                "   `base_day` date ,\n" +
                " `tag_value`VARCHAR(20)  ,\n" +
                "  `g_key` VARCHAR(20)  " +
                ") WITH ( " +
                "      'connector' = 'datagen'\n" +
                "      )");

        tableEnv.executeSql("CREATE TABLE p8_sink (\n" +
                " `tag_code` VARCHAR(250) ,\n" +
                "  `distinct_id` VARCHAR(250)  ,\n" +
                "   `base_day` date ,\n" +
                " `tag_value`VARCHAR(20)  ,\n" +
                "  `g_key` VARCHAR(20)  " +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '192.168.2.131:8034',\n" +
                "      'table.identifier' = 'portrait_44.p8',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123456'\n" +
                ")");
        TableResult tableResult = tableEnv
                .executeSql("  select tag_code,distinct_id,base_day,tag_value,g_key from p8_source");

        tableResult.print();
        env.execute();

    }
}

