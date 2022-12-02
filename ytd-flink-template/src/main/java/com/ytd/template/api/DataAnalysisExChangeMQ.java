package com.ytd.template.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.ytd.template.bean.RabbitMqSinkProperties;
import com.ytd.template.bean.RouteDataProd;
import com.ytd.template.util.mq.AnalysisExChangeMQSinkFunction;
import com.ytd.template.util.ReadProperties;
import com.ytd.template.util.mq.YTDRMQSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * mq数据解析
 * 1.事件
 */
public class DataAnalysisExChangeMQ {
    private static final String ACTIVE = "prod";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10000);

        env.setParallelism(1);
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
                        new SimpleStringSchema()));
                  // deserialization schema to turn messages into Java objects

        //测流输出

        SingleOutputStreamOperator<String> streamAll =
                stream.flatMap(new FlatMapFunction<String, String>() {
                    List<RouteDataProd> routeDataProd = null;
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        routeDataProd = JSONArray.parseArray(value, RouteDataProd.class);
                        if (routeDataProd!=null&&routeDataProd.size() > 0) {
                            for (RouteDataProd route : routeDataProd) {
                                out.collect(JSON.toJSONString(route));
                            }
                        }
                    }
                }).setParallelism(2);

        RabbitMqSinkProperties sinkProperties = RabbitMqSinkProperties.builder()
                .host(ReadProperties.getProperty("mq.event.source.server.host",ACTIVE))
                .port(Integer.parseInt(ReadProperties.getProperty("mq.event.source.server.port",ACTIVE)))
                .userName(ReadProperties.getProperty("mq.event.source.server.username",ACTIVE))
                .passWord(ReadProperties.getProperty("mq.event.source.server.password",ACTIVE))
                .queue(ReadProperties.getProperty("mq.event.source.server.queueNames",ACTIVE))
                .virtualHost(ReadProperties.getProperty("mq.event.source.server.vhost",ACTIVE))
                .exchange(ReadProperties.getProperty("mq.event.source.exchange",ACTIVE))
                .routingkey(ReadProperties.getProperty("mq.event.source.exchange.routingkey",ACTIVE))
                .build();

        streamAll.addSink(new AnalysisExChangeMQSinkFunction(sinkProperties)).setParallelism(2);


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
