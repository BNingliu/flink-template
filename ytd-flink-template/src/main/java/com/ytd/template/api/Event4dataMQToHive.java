package com.ytd.template.api;

import com.alibaba.fastjson.JSONObject;
import com.ytd.template.bean.RouteDataProd;
import com.ytd.template.util.ReadProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Event4dataMQToHive {

    private static final String ACTIVE = "dev";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        //设置活动平台用户旅程MQ（存入集群）
        String queName = ReadProperties.getProperty("mq.event.source.server.hive.queueName",ACTIVE);
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.event.source.server.host",ACTIVE))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.event.source.server.port",ACTIVE)))
                .setUserName(ReadProperties.getProperty("mq.event.source.server.username",ACTIVE))
                .setPassword(ReadProperties.getProperty("mq.event.source.server.password",ACTIVE))
                .setVirtualHost(ReadProperties.getProperty("mq.event.source.server.vhost",ACTIVE))
                .build();


        try {
            final DataStream<String> stream = env.addSource(new RMQSource<String>(
                            connectionConfig,            // config for the RabbitMQ connection
                            queName,                 // name of the RabbitMQ queue to consume
                            false,                        // use correlation ids; can be false if only at-least-once is required
                            new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                    .setParallelism(1);

            SingleOutputStreamOperator<RouteDataProd> routeDataProdStream = stream.map(m -> {
                return JSONObject.parseObject(m, RouteDataProd.class);
            }).setParallelism(2);

            //创建表环境
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            String name = ReadProperties.getProperty("hive.catalogName",ACTIVE);
            HiveCatalog hive = new HiveCatalog(
                    name,
                    ReadProperties.getProperty("hive.defaultDatabase",ACTIVE),
                    ReadProperties.getProperty("hive.hiveConfDir",ACTIVE),
                    ReadProperties.getProperty("hive.version",ACTIVE)
            );
            tableEnv.registerCatalog(name, hive);
            tableEnv.useCatalog(name);
            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            tableEnv.useDatabase(ReadProperties.getProperty("hive.defaultDatabase",ACTIVE));
            tableEnv.createTemporaryView("event4_data_view", routeDataProdStream);

            String createTable =
                    "CREATE  TABLE if not exists act_data_send_event_flink_sink " +
                            "(\n" +
                            "   `routeType` INT, \n" +
                            "   `activityId` STRING, \n" +
                            "  `occurrenceTime`STRING, \n" +
                            "   `accountId` STRING,  \n" +
                            "    `sceneId` STRING, \n " +
                            "   `assemblyName` STRING, \n " +
                            " `ip` STRING, \n " +
                            "  `channel` INT, \n " +
                            " `tenantId` STRING,  \n" +
                            " `wxAppId` STRING,  \n" +
                            " `actionId` STRING,  \n" +
                            " `apiName` STRING,  \n" +
                            " `module` STRING, \n" +
                            " `traceId` STRING, \n" +
                            " `actionResult` STRING, \n" +
                            " `channelCode` STRING \n" +
                            ") partitioned by (createday string,hr string) \n" +
                            "stored as parquet \n" +
                            "TBLPROPERTIES (\n" +
                            "  'partition.time-extractor.timestamp-pattern'='$createday $hr:00:00',\n" +
                            "  'sink.partition-commit.delay'='10s',\n" +
                            "  'sink.partition-commit.trigger'='partition-time',\n" +
                            "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
                            "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                            ")";
            tableEnv.executeSql(createTable);

            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

            String insertSql = String.format(
                    " insert into act_data.act_data_send_event_flink_sink \n" +
                            " SELECT `routeType`, `activityId`, `occurrenceTime`, `accountId`, `sceneId`, `assemblyName`, `ip`, `channel`, `tenantId`, `wxAppId`, `actionId`, `apiName`, `module`, `traceId`, `actionResult`, `channelCode`,\n" +
                            " FROM_UNIXTIME(cast(occurrenceTime as bigint)/1000, 'yyyy-MM-dd')  createday, FROM_UNIXTIME(cast(occurrenceTime as bigint)/1000, 'HH') hr " +
                            " FROM event4_data_view");
            tableEnv.executeSql(insertSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
