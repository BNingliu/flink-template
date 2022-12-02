package com.ytd.template.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytd.template.bean.RabbitMqSinkProperties;
import com.ytd.template.bean.ResonanceEvents;
import com.ytd.template.bean.RouteDataProd;
import com.ytd.template.util.EventMQSinkFunction;
import com.ytd.template.util.ReadProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class RabbitMQToRabbitMQ {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // checkpointing is required for exactly-once or at-least-once guarantees
        env.enableCheckpointing(1000L);

        String queName = ReadProperties.getProperty("mq.event.source.server.queueName");
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.event.server.host"))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.event.server.port")))
                .setUserName(ReadProperties.getProperty("mq.event.server.username"))
                .setPassword(ReadProperties.getProperty("mq.event.server.password"))
                .setVirtualHost(ReadProperties.getProperty("mq.event.server.vhost"))
                .build();


        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        //传入参数：事件实体 Event
        //System.out.println("aaaaaaaaaaa-args:"+args[0]);
        JSONObject sourceEventJson = JSONObject.parseObject(args[0]);
        //Connection connection = MysqlJdbcUtil.getConnection();

        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        queName,                 // name of the RabbitMQ queue to consume
                        false,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                .setParallelism(1);

        //测流输出
        OutputTag<ResonanceEvents> stringOutputTag = new OutputTag<>("event-data",Types.POJO(ResonanceEvents.class));

        SingleOutputStreamOperator<String> validationSingleOutputStreamOperator = stream.process(new ProcessFunction<String, String>() {
            //List<RouteDataProd> routeDatas = null;
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                //routeDatas = JSONArray.parseArray(s, RouteDataProd.class);
                RouteDataProd routeData =JSONObject.parseObject(s,RouteDataProd.class);
                //System.out.println("pppppppppp-routeDatas:"+routeDatas);
                //if (routeDatas != null && routeDatas.size() > 0) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //for (RouteDataProd routeData : routeDatas) {
                        ResonanceEvents resonanceEvents = new ResonanceEvents();
                        resonanceEvents.setEventId(sourceEventJson.getString("code"));
                        resonanceEvents.setEventName(sourceEventJson.getString("name"));
                        resonanceEvents.setOccurrenceTime(sdf.format(new Date(Long.valueOf(routeData.getOccurrenceTime()))));
                        resonanceEvents.setChannelCode(routeData.getChannelCode());
                        resonanceEvents.setAccountId(routeData.getAccountId());
                        ArrayList<JSONObject> sinkAttributesJSONList = new ArrayList<>();
                        for (Object attributesJSON : JSON.parseArray(sourceEventJson.getString("attributesJSON"))) {
                            JSONObject acceptAttributesJSONObject = JSONObject.parseObject(attributesJSON.toString());
                            JSONObject routeDataJsonObject = (JSONObject) JSONObject.toJSON(routeData);
                            if (routeDataJsonObject.containsKey(acceptAttributesJSONObject.get("attributeId"))) {
                                JSONObject sinkAttributesJSON = new JSONObject();
                                sinkAttributesJSON.put("attributeId", acceptAttributesJSONObject.get("attributeId"));
                                sinkAttributesJSON.put("value", routeDataJsonObject.getString(acceptAttributesJSONObject.getString("attributeId")));
                                sinkAttributesJSONList.add(sinkAttributesJSON);
                            } else {
                                //字段不对应，flink任务抛异常
                                throw new RuntimeException("未包含此扩展属性！");
                            }
                        }
                        resonanceEvents.setAttributesJSON(sinkAttributesJSONList.toString());
                        String resonanceEventJson = JSON.toJSONString(resonanceEvents);
                        collector.collect(resonanceEventJson);
                        context.output(stringOutputTag,resonanceEvents);
                    //}
                //}
            }
        }).setParallelism(1);


        RabbitMqSinkProperties sinkProperties = RabbitMqSinkProperties.builder()
                .host(ReadProperties.getProperty("mq.event.server.host"))
                .port(Integer.parseInt(ReadProperties.getProperty("mq.event.server.port")))
                .userName(ReadProperties.getProperty("mq.event.server.username"))
                .passWord(ReadProperties.getProperty("mq.event.server.password"))
                .queue(ReadProperties.getProperty("mq.event.sink.server.queueName"))
                .virtualHost(ReadProperties.getProperty("mq.event.server.vhost"))
                .build();


        //测流输出到hive
        DataStream<ResonanceEvents> hiveStream = validationSingleOutputStreamOperator.getSideOutput(stringOutputTag);


        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String name = ReadProperties.getProperty("hive.catalogName");
        HiveCatalog hive = new HiveCatalog(
                name,
                ReadProperties.getProperty("hive.defaultDatabase"),
                ReadProperties.getProperty("hive.hiveConfDir"),
                ReadProperties.getProperty("hive.version")
        );
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase(ReadProperties.getProperty("hive.defaultDatabase"));
        tableEnv.createTemporaryView("mq_event_view", hiveStream);

        String createTable =
                "CREATE  TABLE if not exists mq_event_sink " +
                        "(\n" +
                        "   `eventId` STRING, \n" +
                        "   `eventName` STRING, \n" +
                        "  `occurrenceTime` STRING, \n" +
                        "   `channelCode` STRING,  \n" +
                        "   `accountId` STRING,  \n" +
                        "    `attributesJSON` STRING \n " +
                        ") partitioned by (dt string) \n" +
                        "stored as parquet \n" +
                        "TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt',\n" +
                        "  'sink.partition-commit.delay'='10s',\n" +
                        "  'sink.partition-commit.trigger'='partition-time',\n" +
                        "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                        ")";
//                "CREATE  TABLE if not exists mq_event_sink " +
//                        "(\n" +
//                        "   `routeType` STRING, \n" +
//                        "   `activityId` STRING, \n" +
//                        "  `occurrenceTime`STRING, \n" +
//                        "   `accountId` STRING,  \n" +
//                        "    `sceneId` STRING, \n " +
//                        "   `assemblyName` STRING, \n " +
//                        " `ip` STRING, \n " +
//                        "  `channel` STRING, \n " +
//                        " `tenantId` STRING,  \n" +
//                        " `wxAppId` BOOLEAN,  \n" +
//                        " `actionId` STRING,  \n" +
//                        " `apiName` STRING,  \n" +
//                        " `module` STRING \n" +
//                        " `traceId` STRING \n" +
//                        " `actionResult` STRING \n" +
//                        " `channelCode` STRING \n" +
//                        ") partitioned by (dt string,hr string) \n" +
//                        "stored as parquet \n" +
//                        "TBLPROPERTIES (\n" +
//                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
//                        "  'sink.partition-commit.delay'='10s',\n" +
//                        "  'sink.partition-commit.trigger'='partition-time',\n" +
//                        "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
//                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
//                        ")";
        tableEnv.executeSql(createTable);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        //"  insert into act_data.mq_event_sink \n" +
        String insertSql = String.format(
                "  insert into act_data.mq_event_sink \n" +
                " SELECT eventId, eventName, occurrenceTime, channelCode,accountId, attributesJSON,\n" +
                " FROM_UNIXTIME(unix_timestamp(), 'yyyy-MM-dd') dt" +
                " FROM mq_event_view");
        tableEnv.executeSql(insertSql);


        //将结果流用自定义的sink发送到rabbitmq
        validationSingleOutputStreamOperator.addSink(new EventMQSinkFunction(sinkProperties));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
