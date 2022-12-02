package com.ytd.template.bean;

import lombok.Data;

import java.util.Date;

/**
 * 埋点原始字段解析字段
 */
@Data
public class UserEventInfo {
    private String id;
    private String userId;
    private String userAgent;
    private String visitOs;
    private String visitBrowser;
    private String visitBrowserVersion;
    private String visitTime;
    private String visitUrl;
    private String visitByChannel;
    private String visitIp;
    private String refererUrl;
    private String projectId;
    private String subject;
    private String eventType;
    private String eventName;
    private String eventValue;
    private String activityId;
    private String activityType;
    private String activityScene;
    private String method;
    private String appId;
    private String url;

	private String tenantId;
	private String eventDetail;
	private String apiUri;
	private String requestBody;
	private String requestResult;
	private long requestCostTime;

}
