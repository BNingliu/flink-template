package com.ytd.template.bean;

import lombok.Data;

/**
 * @program: ytd-flink-template
 * @description:  校验数据
 * @author: liuningbo
 * @create: 2022/09/13 11:43
 */
@Data
public class YTDValidation {
//    id STRING,   traceid STRING,   activityid STRING,   accountid STRING,
//    isdeleted BOOLEAN,   createtime BIGINT,   appid STRING,
//    actionid STRING,   passed BOOLEAN,   ruleid STRING,
//    value STRING,   level INT
//    ) PARTITIONED BY (
//    tenantid STRING,    createday STRING

    private  String id;
    private  String traceid;
    private  String activityid;
    private  String accountid;
    private  Boolean isdeleted;
    private  Long createtime;
    private  String appid;
    private  String actionid;
    private  Boolean passed;
    private  String ruleid;
    private  String value;
    private  Integer level;
    private  String tenantid;

}
