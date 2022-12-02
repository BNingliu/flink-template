package com.ytd.template.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.ytd.template.bean.RouteDataProd;
import com.ytd.template.util.ReadProperties;
import com.ytd.template.util.mq.YTDRMQSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * mq数据解析
 * 1.事件
 */
@Deprecated
public class DataAnalysisMQToMQ {
    private static final String ACTIVE = "prod";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10000);

        env.setParallelism(2);
        //关闭共享slot(可以观察到消息发送和接收数量)]
        env.disableOperatorChaining();

        //设置活动平台用户旅程MQ
        String queName = ReadProperties.getProperty("mq.customerjourney.server.queueName",ACTIVE);
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.customerjourney.server.host",ACTIVE))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.customerjourney.server.port",ACTIVE)))
                .setUserName(ReadProperties.getProperty("mq.customerjourney.server.username",ACTIVE))
                .setPassword(ReadProperties.getProperty("mq.customerjourney.server.password",ACTIVE))
                .setVirtualHost(ReadProperties.getProperty("mq.customerjourney.server.vhost",ACTIVE))
                .build();



        final DataStream<String> stream = env
                .addSource(new YTDRMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        queName,                 // name of the RabbitMQ queue to consume
                        false,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))
                .setParallelism(1);  // deserialization schema to turn messages into Java objects

        //测流输出
        OutputTag<String> eventRabbitMq1 = new OutputTag<>("event-source-data",Types.STRING);

        //colltor输出到事件库mq；context测流输出到Hive表
        SingleOutputStreamOperator<String> eventDataAll = stream.process(new ProcessFunction<String, String>() {
            List<RouteDataProd> routeDatas = null;
            @Override
            public void processElement(String value, Context context, Collector<String> collector) throws Exception {
                routeDatas = JSONArray.parseArray(value, RouteDataProd.class);
                    for (RouteDataProd routeData : routeDatas) {
                        collector.collect(JSON.toJSONString(routeData));
                        context.output(eventRabbitMq1,JSON.toJSONString(routeData));
                    }
                }
        });

        RMQConnectionConfig sinkProperties = new RMQConnectionConfig.Builder()
                .setHost(ReadProperties.getProperty("mq.event.source.server.host",ACTIVE))
                .setPort(Integer.parseInt(ReadProperties.getProperty("mq.event.source.server.port",ACTIVE)))
                .setUserName(ReadProperties.getProperty("mq.event.source.server.username",ACTIVE))
                .setPassword(ReadProperties.getProperty("mq.event.source.server.password",ACTIVE))
                .setVirtualHost(ReadProperties.getProperty("mq.event.source.server.vhost",ACTIVE))
                .build();

        DataStream<String> eventRabbitStream01 = eventDataAll.getSideOutput(eventRabbitMq1);

        //共鸣事件
        eventDataAll.addSink(new RMQSink<String>(sinkProperties, ReadProperties.getProperty("mq.event.source.server.gm.queueName",ACTIVE),  new SimpleStringSchema()));
        //hive存储
        eventRabbitStream01.addSink(new RMQSink<String>(sinkProperties, ReadProperties.getProperty("mq.event.source.server.hive.queueName",ACTIVE),  new SimpleStringSchema()));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
