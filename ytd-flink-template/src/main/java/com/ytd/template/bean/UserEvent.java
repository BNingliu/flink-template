package com.ytd.template.bean;

import lombok.Data;

import java.util.Date;

/**
 * @program: ytd-flink-template
 * @description: 埋点数据
 * @author: liuningbo
 * @create: 2022/11/25 13:49
 */
@Data
public class UserEvent {

    private String id ;
    private String userid ;
    private String useragent ;
    private String visitos ;
    private String visitbrowser ;
    private String visitbrowserversion ;
    private Date visittime ;
    private String visiturl ;
    private String visitbychannel ;
    private String visitip ;
    private String refererurl ;
    private String projectid ;
    private String subject ;
    private String eventtype ;
    private String eventname ;
    private String eventvalue ;
    private String activityid ;
    private String activitytype ;
    private String activityscene ;
    private String method ;
    private String appid ;
    private String url ;
    private String tenantid ;
    private String eventdetail ;
    private String apiuri ;
    private String requestbody ;
    private String requestresult ;
    private String requestcosttime ;
}
