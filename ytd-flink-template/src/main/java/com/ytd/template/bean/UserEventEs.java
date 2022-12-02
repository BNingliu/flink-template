package com.ytd.template.bean;

import lombok.Data;

import java.util.Date;

/**
 * @program: ytd-flink-template
 * @description: 埋点数据ES
 * @author: liuningbo
 * @create: 2022/11/25 13:49
 */
@Data
public class UserEventEs {

    private String id ;
    private String visitip ;
    private String visitbychannel ;
    private String appId ;
    private String visiturl ;
    private String eventname ;
    private Long requestcosttime ;
    private String userId ;
    private String url ;
    private String visittime ;
    private String projectid ;

}
