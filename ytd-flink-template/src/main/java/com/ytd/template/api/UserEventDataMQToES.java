package com.ytd.template.api;

import com.alibaba.fastjson.JSONObject;
import com.ytd.template.bean.TraceInfo;
import com.ytd.template.bean.UserEventInfo;
import com.ytd.template.util.ReadProperties;
import com.ytd.template.util.event.ParseTraceInfoUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
public class UserEventDataMQToES {

    private static final String ACTIVE = "dev";

    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        //接收用户旅程次级MQ
        String queName =ReadProperties.getProperty("es.user.event.queueName",ACTIVE) ;
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.event.source.server.host",ACTIVE))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.event.source.server.port",ACTIVE)))
                .setUserName(ReadProperties.getProperty("mq.event.source.server.username",ACTIVE))
                .setPassword(ReadProperties.getProperty("mq.event.source.server.password",ACTIVE))
                .setVirtualHost(ReadProperties.getProperty("mq.event.source.server.vhost",ACTIVE))
                .build();


        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        queName,                 // name of the RabbitMQ queue to consume
                        false,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                .setParallelism(1);

        //输出到Hive表
        SingleOutputStreamOperator<UserEventInfo> validationSingleOutputStreamOperator = stream.process(
                new ProcessFunction<String, UserEventInfo>() {
            @Override
            public void processElement(String s, Context context, Collector<UserEventInfo> collector) throws Exception {
                TraceInfo traceInfo = JSONObject.parseObject(s, TraceInfo.class);
                collector.collect(ParseTraceInfoUtil.parseTraceInfo(traceInfo, false));
            }
        }).setParallelism(2);
        validationSingleOutputStreamOperator.print();
        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("trace_info", validationSingleOutputStreamOperator);

        String createTable =
                "CREATE TABLE userEvent (\n" +
                        "  id STRING,\n" +
                        "  visitip STRING,\n" +
                        "  visitbychannel STRING,\n" +
                        "  appId STRING,\n" +
                        "  visiturl STRING,\n" +
                        "  eventname STRING,\n" +
                        "  requestcosttime BIGINT,\n" +
                        "  userId STRING,\n" +
                        "  url STRING,\n" +
                        "  visitTime TIMESTAMP,\n" +
                        "  projectid STRING,\n" +
                        "  appid STRING,\n" +
                        "  useragent String  ,\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'elasticsearch-6',\n" +
                        "  'hosts' = '"+ReadProperties.getProperty("es.hosts",ACTIVE)+"',\n" +
                        "  'document-type' = '"+ReadProperties.getProperty("es.document.type",ACTIVE)+"',\n" +
                        "  'index' = '"+ReadProperties.getProperty("es.index",ACTIVE)+"'\n" +
              ")";
        tableEnv.executeSql(createTable);


        String insertSql = String.format(
                " insert into userEvent \n" +
                        " SELECT `id`, `visitIp`, `visitByChannel`, `appId`, `visitUrl`, `eventName`, `requestCostTime`," +
                        " `userId`, `url`,TO_TIMESTAMP(`visitTime`, 'yyyy-MM-dd HH:mm:ss') as vTime , `projectId`, `appId`," +
                        " CONCAT(projectId, '-',DATE_FORMAT(visitTime, 'yyyy-MM') )   as useragent  " +
                        " FROM trace_info"
        );
        tableEnv.executeSql(insertSql).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
