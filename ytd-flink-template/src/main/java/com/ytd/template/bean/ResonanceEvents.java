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
public class ResonanceEvents {

    private  String eventId; //事件ID
    private  String eventName; //事件名称
    private String occurrenceTime; //事件发生时间
    private  String channelCode; //来访渠道编码
    private String accountId; //用户id
    /**
     * 事件扩展属性，以JSON列表格式存储
     * [{
     * "attribute_name": "属性名称",
     * "attribute_id": "属性ID",
     * "type": "属性数据类型", NUMBER, BOOL, STRING, DATATIME, LIST
     * "unit": "度量单位",
     * "attribute_desc": "属性说明"
     * },
     * {
     * "attribute_name": "属性名称",
     * "attribute_id": "属性ID",
     * "type": "属性数据类型", NUMBER, BOOL, STRING, DATATIME, LIST
     * "unit": "度量单位",
     * "attribute_desc": "属性说明"
     *  }]
     */
    private  String attributesJSON; //事件扩展属性

    @Override
    public String toString() {
        return "ResonanceEvents{" +
                "eventId='" + eventId + '\'' +
                ", eventName='" + eventName + '\'' +
                ", occurrenceTime=" + occurrenceTime +
                ", channelCode='" + channelCode + '\'' +
                ", attributesJSON='" + attributesJSON + '\'' +
                '}';
    }
}
