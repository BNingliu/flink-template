package com.ytd.template.api;

import com.alibaba.fastjson.JSONArray;
import com.ytd.template.bean.YTDValidation;
import com.ytd.template.util.ReadProperties;
import com.ytd.template.util.mq.YTDRMQSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @program: ytd-flink-template
 * @description: 活动校验数据至hive
 * @author: liuningbo
 */
public class YTDValidationToHive {
    private static final String ACTIVE = "prod";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        System.setProperty("HADOOP_USER_NAME", ReadProperties.getProperty("hdfs.username",ACTIVE));

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(30000);  //头和头
        env.getCheckpointConfig().setCheckpointTimeout(90000);

        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        String queName = ReadProperties.getProperty("mq.validation.server.queueName",ACTIVE);
        env.setParallelism(1);
        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.validation.server.host",ACTIVE))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.validation.server.port",ACTIVE)))
                .setUserName(ReadProperties.getProperty("mq.validation.server.username",ACTIVE))
                .setPassword(ReadProperties.getProperty("mq.validation.server.password",ACTIVE))
                .setVirtualHost(ReadProperties.getProperty("mq.validation.server.vhost",ACTIVE))
                .build();

//        RMQSource
        final DataStream<String> stream = env
                .addSource(new YTDRMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        queName,                 // name of the RabbitMQ queue to consume
                        false,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))// deserialization schema to turn messages into Java objects
                .setParallelism(1);              // non-parallel source is only required for exactly-once



        SingleOutputStreamOperator<YTDValidation> validationSingleOutputStreamOperator =
                stream.flatMap(new FlatMapFunction<String, YTDValidation>() {
             List<YTDValidation> ytdValidations = null;

            @Override
            public void flatMap(String value, Collector<YTDValidation> out) throws Exception {
                ytdValidations = JSONArray.parseArray(value, YTDValidation.class);
                if (ytdValidations!=null&&ytdValidations.size() > 0) {
                    for (YTDValidation ytdValidation : ytdValidations) {
                        out.collect(ytdValidation);
                    }
                }
            }
        }).setParallelism(4);
//        validationSingleOutputStreamOperator.print();
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
        tableEnv.createTemporaryView("ytd_validation_view", validationSingleOutputStreamOperator);

        String createTable =
                "CREATE  TABLE if not exists act_validation_log_sink " +
                        "(\n" +
                        "   `id` STRING, \n" +
                        "   `traceid` STRING, \n" +
                        "  `activityid`STRING, \n" +
                        "   `accountid` STRING,  \n" +
                        "    `isdeleted` BOOLEAN, \n " +
                        "   `createtime` BIGINT, \n " +
                        " `appid` STRING, \n " +
                        "  `tenantid` STRING, \n " +
                        " `actionid` STRING,  \n" +
                        " `passed` BOOLEAN,  \n" +
                        " `ruleid` STRING,  \n" +
                        " `value` STRING,  \n" +
                        " `level` INT \n" +
                        ") partitioned by (dt string,hr string) \n" +
                        "stored as parquet \n" +
                        "TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                        "  'sink.partition-commit.delay'='10s',\n" +
                        "  'sink.partition-commit.trigger'='partition-time',\n" +
                        "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                        ")";
        tableEnv.executeSql(createTable);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String insertSql = String.format("  insert into act_data.act_validation_log_sink " +
                " SELECT id, traceid, activityid, accountid, isdeleted, createtime, appid, tenantid, actionid, passed, ruleid, `value`, `level` , " +
                " FROM_UNIXTIME(createtime/1000, 'yyyy-MM-dd') dt,  FROM_UNIXTIME(createtime/1000, 'HH') hr" +
                " FROM ytd_validation_view");
        tableEnv.executeSql(insertSql);

//        env.execute();

    }

}
