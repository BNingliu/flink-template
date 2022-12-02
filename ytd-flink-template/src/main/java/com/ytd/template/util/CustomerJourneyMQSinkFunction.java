package com.ytd.template.util;

import com.alibaba.fastjson.JSONObject;
import com.ytd.template.bean.RabbitMqSinkProperties;
import com.ytd.template.bean.RouteDataProd;

import java.nio.charset.StandardCharsets;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/08/16 21:28
 */
public class CustomerJourneyMQSinkFunction extends DataRichSinkFunction<String> {

    private  JSONObject jsonObject = null;
    public CustomerJourneyMQSinkFunction(RabbitMqSinkProperties rabbitMQSinkProperties) {
        /**
         * 调用父类（DataRichSinkFunction）构造，完成父类中属性填充
         */
        super(rabbitMQSinkProperties);
    }

//
//  private  AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties
//            .Builder();


    /**
     * 数据输出到 rabbitMQSinkProperties 指定的交换机中
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
//        System.out.println(LocalDateTime.now() + "发送数据：" + value);
        channel.basicPublish("",
                ReadProperties.getProperty("mq.event.source.server.queueName")
                ,null ,
                value.getBytes(StandardCharsets.UTF_8)
        );
    }
}

