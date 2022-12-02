package com.ytd.template.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.ytd.template.bean.RabbitMqSinkProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @desc mq 公共模板类 我们我们如果有多个mq-sink  只需继承此类 即可
 */
@Slf4j
public class DataRichSinkFunction<IN> extends RichSinkFunction<IN> {

    // 配置对象  后续我们在定义具体实体类时用子类触发父类构造调用
    protected final RabbitMqSinkProperties rabbitMQSinkProperties;



    protected Connection connection;

    protected Channel channel;

    public DataRichSinkFunction(RabbitMqSinkProperties rabbitMQSinkProperties) {
        this.rabbitMQSinkProperties = rabbitMQSinkProperties;
    }


    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();

        // 设置RabbitMQ相关信息
        factory.setHost(rabbitMQSinkProperties.getHost());
        factory.setUsername(rabbitMQSinkProperties.getUserName());
        factory.setPassword(rabbitMQSinkProperties.getPassWord());
        factory.setPort(rabbitMQSinkProperties.getPort());
        factory.setVirtualHost(rabbitMQSinkProperties.getVirtualHost());
        // 创建一个新的连接
        connection = factory.newConnection();
        // 创建一个通道
        channel = connection.createChannel();



        // 声明一个队列
        channel.queueDeclare(rabbitMQSinkProperties.getQueue(), true, false, false, null);

        // 声明交换机
//        channel.exchangeDeclare(rabbitMQSinkProperties.getExchange(), BuiltinExchangeType.FANOUT, true);
    }

    /**
     * 关闭连接 flink程序从启动到销毁只会执行一次
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        channel.close();
        connection.close();
    }

}
