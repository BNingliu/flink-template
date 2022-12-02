package com.ytd.template.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/08/16 21:13
 */
@Builder
@Data
public class RabbitMqSinkProperties implements Serializable {
    /**
     * rabbitMQ ip
     */
    private String host;
    /**
     * 端口
     */
    private int port;
    /**
     * 用户民
     */
    private String userName;
    /**
     * 密码
     */
    private String passWord;
    /**
     * 交换机名
     */
    private String exchange;

    /**
     * 队列名
     */
    private String queue;

    private String virtualHost;
    private String routingkey;

}

