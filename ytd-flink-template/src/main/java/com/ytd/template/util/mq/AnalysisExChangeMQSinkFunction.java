package com.ytd.template.util.mq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.ytd.template.bean.RabbitMqSinkProperties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.nio.charset.StandardCharsets;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/08/16 21:28
 */
public class AnalysisExChangeMQSinkFunction extends RichSinkFunction<String> {

    // 配置对象  后续我们在定义具体实体类时用子类触发父类构造调用
    private  RabbitMqSinkProperties rabbitMQSinkProperties;
    private Channel channel;
    private Connection connection;
    public AnalysisExChangeMQSinkFunction(RabbitMqSinkProperties rabbitMQSinkProperties) {
        this.rabbitMQSinkProperties=rabbitMQSinkProperties;
    }


    /**
     * 数据输出到 rabbitMQSinkProperties 指定的交换机中
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        /**
         * 发送消息到指定队列
         * 参数1  交换机名称
         * 参数2  消息的RoutingKey，如果这个消息的RoutingKey和某个队列与交换机绑定的RoutingKey一致，
         *      那么这个消息就会发送的指定的队列中
         * 参数3  消息属性信息，通常为null
         * 参数4  具体的消息数据的字节数组
         *
         * 注：发送消息时必须确保交换机已经创建，并且已经绑定到了某个队列
         */
        channel.basicPublish(rabbitMQSinkProperties.getExchange(),rabbitMQSinkProperties.getRoutingkey()
                ,null ,
                value.getBytes(StandardCharsets.UTF_8)
        );
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

        //获取连接
        connection = factory.newConnection();
        //获取通道
        channel = connection.createChannel();

        /**
         * 声明一个队列
         * 参数1 队列名称取值任意
         * 参数2 是否为持久化的队列
         * 参数3 是否排外，如果排外这个队列只允许一个消费者监听
         * 参数4 是否自动删除队列，如果为true表示当前队列中没有消息，也没有消费者连接时就会自动删除这个队列
         * 参数5 队列的一些属性设置，通常为null
         *
         * 注：
         * 1、声明队列时，这个队列名称如果已经存在则放弃声明，如果队列不存在则会声明一个新的队列
         * 2、队列名可以取值任意，但是要与消息接收时的队列名称完全一致
         * 3、一定要在发送消息前确认队列名已经存在在RabbitMQ中，否则就会出现问题（如下代码可有可无）
         */
        // 声明一个队列
        for (String queue : rabbitMQSinkProperties.getQueue().split(",")) {
            channel.queueDeclare(queue, true, false, false, null);
        }

        /**
         * 声明一个交换机
         * 参数1  交换机名称
         * 参数2  交换机类型，取值：direct，fanout，topic，headers
         * 参数3  为是否为持久化交换机
         * 注：
         * 1、声明交换机时如果这个交换机应存在则会放弃声明，如果交换机不存在则声明交换机
         * 2、在使用前必须要确保这个交换机被声明（如下代码可有可无）
         */
        channel.exchangeDeclare(rabbitMQSinkProperties.getExchange(), BuiltinExchangeType.FANOUT,true);

        /**
         * 将队列绑定到交换机
         * 参数1  队列名称
         * 参数2  交换机名称
         * 参数3  消息的RoutingKey（也是BindingKey）
         *
         * 注：在进行队列和交换机绑定时必须确保队列和交换机已经成功声明
         */
        // 声明一个队列
        for (String queue : rabbitMQSinkProperties.getQueue().split(",")) {
            channel.queueBind(queue,rabbitMQSinkProperties.getExchange(),rabbitMQSinkProperties.getRoutingkey());
        }
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

