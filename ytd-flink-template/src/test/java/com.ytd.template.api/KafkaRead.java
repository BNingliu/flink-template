package com.ytd.template.api;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.util.Optional;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/08/05 13:33
 */
public class KafkaRead {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        traceId : "561366462707617792"
//        activityId : "558826197555941376"
//        accountId : "557401174907588608"
//        apiName : "点击公众号菜单"
//        channel : 1
//        tenantId : "100000000000000001"
//        wxAppId : "wx0cba72caa9bd748d"
//        occurrenceTime : 1658131335409
//        routeType : -5
        String tableName =

                " CREATE TABLE kafka_cdc\n" +
                "(\n" +
                "    traceId    STRING ,\n" +
                "   activityId  STRING,\n" +
                "   accountId   STRING,\n" +
                "   apiName     STRING,\n" +
                "   channel     int,\n" +
                "   tenantId    STRING ,\n" +
                "   wxAppId     STRING ,\n" +
                "   occurrenceTime   bigint,\n" +
                "   routeType       int \n" +
                ") WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'yeyoo01', \n" + //-- 指定消费的topic
                "      'scan.startup.mode' = 'earliest-offset', \n" + //-- 指定起始offset位置, latest-offset
                "      'properties.zookeeper.connect' = '192.168.2.131:2182',\n" +
                "      'properties.bootstrap.servers' = '192.168.2.131:9095,192.168.2.131:9096,192.168.2.131:9097',\n" +
//                "--       'connector.properties.group.id' = 'student_1',\n" +
                "      'format' = 'json' \n" +
                "      )";

        try {
            tableEnv.executeSql(tableName);
            Table table = tableEnv.sqlQuery("select * from kafka_cdc");

            DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class).filter(f -> {
                System.out.println(f.f1.toString());
                return f.f0.toString().equals("true");
            } );
            retractStream.print();
//            DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
//            rowDataStream.print();
//            DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
//            rowDataStream.print();
            String name = "myhive";
            String defaultDatabase = "di_20";
            String hiveConfDir = "hdfs://ytddata0.localdomain:8020/applications/flink/flink-libs/conf/";

            String version = "3.1.0";
            HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
            tableEnv.registerCatalog("myhive", hiveCatalog);
            tableEnv.useCatalog("myhive");
            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            tableEnv.useDatabase(defaultDatabase);
            tableEnv.createTemporaryView("ytd_user_group", retractStream);


            tableEnv.executeSql("drop table if exists hive_table3");
            //      如果hive中已经存在了相应的表，则这段代码省略
            String hiveSql = "CREATE external TABLE if not exists  hive_table3 (\n" +
                    " traceId    STRING, \n" +
                    " activityId  STRING, \n" +
                    "   accountId   STRING, \n" +
                    "  apiName     STRING,  \n" +
                    "  occurrenceTime   bigint " +
                    " )  stored as parquet TBLPROPERTIES (\n" +
//                    "  'partition.time-extractor.timestamp-pattern'='$dt $h:00:00',\n" +
                    "  'sink.partition-commit.delay'='10s',\n" +
                    "  'sink.partition-commit.trigger'='partition-time',\n" +
                    "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
                    "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                    ")";
            tableEnv.executeSql(hiveSql);

            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

            TableResult result = tableEnv.executeSql(
                    " insert into hive_table3  SELECT " +
                            "  f1.traceId," +
                            "  f1.activityId ," +
                            "  f1.accountId, " +
                            "  f1.apiName," +
                            "  f1.occurrenceTime" +
                            " FROM ytd_user_group");
            result.print();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
