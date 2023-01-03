package com.ytd.template.api;


import com.alibaba.fastjson.JSONArray;
import com.ytd.template.bean.RouteDataProd;
import com.ytd.template.util.ReadProperties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class MqToGreenPlum {
    private static final String ACTIVE = "dev";

    public static void main(String[] args) throws Exception {
//1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);  //头和头
        //设置statebackend
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        //1.1 开启CK并指定状态后端为FS
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://192.168.2.131:8020/applications/flink/flink-checkpoints/40"));

        env.setParallelism(2);
        env.disableOperatorChaining();
        String queName = ReadProperties.getProperty("mq.event.source.server.queueName", ACTIVE);
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.event.server.host", ACTIVE))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.event.server.port", ACTIVE)))
                .setUserName(ReadProperties.getProperty("mq.event.server.username", ACTIVE))
                .setPassword(ReadProperties.getProperty("mq.event.server.password", ACTIVE))
                .setVirtualHost(ReadProperties.getProperty("mq.event.server.vhost", ACTIVE))
                .build();

        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        queName,                 // name of the RabbitMQ queue to consume
                        false,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                .setParallelism(1);

        SingleOutputStreamOperator<RouteDataProd> streamAll =
                stream.flatMap(new FlatMapFunction<String, RouteDataProd>() {
                    List<RouteDataProd> routeDataProd = null;
                    @Override
                    public void flatMap(String value, Collector<RouteDataProd> out) throws Exception {
                        routeDataProd = JSONArray.parseArray(value, RouteDataProd.class);
                        if (routeDataProd != null && routeDataProd.size() > 0) {
                            for (RouteDataProd route : routeDataProd) {
                                out.collect(route);
                            }
                        }
                    }
                }).setParallelism(2);
        tableEnv.createTemporaryView("event4_data_view", streamAll);

        String createTable =
                "CREATE TABLE  act_data_send_event_flink_sink " +
                        "(\n" +
                        "   `routetype`  INT, \n" +
                        "   `activityid`  String, \n" +
                        "  `occurrencetime` String, \n" +
                        "   `accountid` String,  \n" +
                        "    `sceneid` String, \n " +
                        "   `assemblyname` String, \n " +
                        " `ip` String, \n " +
                        "  `channel` INT, \n " +
                        " `tenantid` String,  \n" +
                        " `wxappid` String,  \n" +
                        " `actionid` String,  \n" +
                        " `apiname` String,  \n" +
                        " `module` String, \n" +
                        " `traceid` String, \n" +
                        " `actionresult` String, \n" +
                        " `channelcode` String ,\n" +
                        " `createday`  String \n" +
                        " ) WITH (\n" +
                        "  'connector' = 'ytd-greenplum',\n" +
                        "  'url' = '168.41.6.66',\n" +
                        "  'port' = '5432',\n" +
                        "  'table-name' = 'act_data_send_event_flink_sink',\n" +
                        "  'database-name' = 'default_login_database_name',\n" +
                        "  'schema-name' = 'public',\n" +
                        "  'max-rows' = '1000',\n" +
                        "  'username' = 'gpadmin',\n" +
                        "  'password' = '4fkBz57a8ZFjSyXs'\n" +
                        "               )";
        tableEnv.executeSql(createTable);

        String insertSql = String.format(
                " insert into act_data_send_event_flink_sink \n" +
                        " SELECT `routeType`, `activityId`, `occurrenceTime`, `accountId`, `sceneId`, `assemblyName`, `ip`, `channel`, `tenantId`, `wxAppId`, `actionId`, `apiName`, `module`, `traceId`, `actionResult`, `channelCode`,\n" +
                        " FROM_UNIXTIME(cast(occurrenceTime as bigint)/1000, 'yyyy-MM-dd')  createday " +
                        " FROM event4_data_view");
        tableEnv.executeSql(insertSql).print();

        env.execute();
    }

}
