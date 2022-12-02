package com.ytd.template.bean;

import lombok.Data;

import java.util.Date;

/**
 * @program: ytd-flink-template
 * @description:  校验数据
 * @author: liuningbo
 * @create: 2022/09/13 11:43
 */
@Data
public class RouteDataProd {

    //旅程类型（RouteTypeConstant）
    private Byte routeType;

    //活动id
    private String activityId;

    //发生时间
    private String occurrenceTime;

    //用户id
    private String accountId;

    //场景id
    private String sceneId;

    //组件名称
    private String assemblyName;

    //ip
    private String ip;

    //来访渠道(AccountTypeConstant)
    private Byte channel;

    //租户id
    private String tenantId;

    //wxAppId
    private String wxAppId;

    //actionId
    private String actionId;

    //api接口名称
    private String apiName;

    //校验类型
    private String module;

    //唯一值为了反串出此行为所能对应出的数据
    private String traceId;

    //最终的结果
    private String actionResult;

    //来访渠道编码
    private String channelCode;

    //页面名称
    private String pageName;

    //浏览时长
    private String eventDuration;


}
