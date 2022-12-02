package com.ytd.template.util.event;

import lombok.Data;

import java.util.Date;

/**
 * Created by aaron.pan on 2017/3/22.
 */

@Data
public class ClientInfo {

    private String userId;

    private String os;

    private String osVersion;

    private String browser;

    private String browserVersion;

    private String url;

    private String controller;

    private String action;

    private Date createTime;

    private String event;

    private String eventType;

    private String subject;

    private String projectId;

    private String clientIP;

    private int clientPort;

    private String skin;

}
