package com.ytd.template.bean;//import org.apache.hadoop.io.Writable;


import lombok.Data;

/**
 * 埋点原始字段
 */
@Data
public class TraceInfo  {
	
    private String deviceId;
    private String iPv4;
    private String iPv6;
    private int port;
    private String mAC;
    private String lng;
    private String lat;
    private String method;
    private String userAgent;
    private String url;
    private String queryString;
    private String requestHeader;
    private String requestBody;
    private String requestSource;
    private String sourceType;
    private String sourceIP;
    private String projectId;
    private String subject;
    private String userId;
    private String eventType;
    private String event;
    private String createTime;
    private String value;
    private String cookie;
    private String referer;
    private String uuid;

}
