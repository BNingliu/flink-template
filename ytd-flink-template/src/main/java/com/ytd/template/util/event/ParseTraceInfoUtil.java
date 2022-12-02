package com.ytd.template.util.event;

import com.alibaba.fastjson.JSONObject;
import com.ytd.template.bean.TraceInfo;
import com.ytd.template.bean.UserEventInfo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/11/29 15:16
 */
public class ParseTraceInfoUtil {

    private  static  UserEventInfo eventInfo =null;
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static UserEventInfo parseTraceInfo(TraceInfo traceInfo, Boolean isFull) throws ParseException {
        if(traceInfo.getProjectId() == null) {
            return null;
        }
        eventInfo = new UserEventInfo();

        eventInfo.setId(traceInfo.getUuid());
        eventInfo.setProjectId(traceInfo.getProjectId());
        eventInfo.setEventName(traceInfo.getEvent());
        JSONObject valueMap = JSONObject.parseObject(traceInfo.getValue());

        eventInfo.setUserId( traceInfo.getUserId());//⭐⭐⭐
        eventInfo.setVisitUrl(traceInfo.getUrl());//⭐⭐⭐

        String url = getRequestUrl(valueMap);
        String actId = getActivityId(valueMap);;
        String actType = getActivityType(valueMap);
        String appId = getAppId(valueMap);
        String tenantId = getTenantId(valueMap);
        String eventDetail = getEventDetail(valueMap);
        String apiUri = getApiUri(valueMap);
        String requestBody = getRequestBody(valueMap);
        String requestResult = getRequestResult(valueMap);
        String requestCostTime = getRequestCostTime(valueMap);

        //旧版数据
        eventInfo.setUrl(url);
        if(eventInfo.getUrl()==null){
            eventInfo.setUrl(traceInfo.getUrl());
        }

        String format = df.format(df.parse(traceInfo.getCreateTime()));
        eventInfo.setVisitTime(format);//⭐⭐⭐
        eventInfo.setAppId(appId);//***


        TraceAnalyze analyze = new TraceAnalyze(traceInfo);

//        if (analyze != null && analyze.getClientInfo() != null) {
//            ClientInfo clientInfo = analyze.getClientInfo();
//            if(isFull) {
//                eventInfo.setVisitBrowser(clientInfo.getBrowser());
//                eventInfo.setVisitBrowserVersion(clientInfo.getBrowserVersion());
//                eventInfo.setVisitOs(clientInfo.getOs());
//            }
//            String ip = clientInfo.getClientIP();
//            if (!Strings.isNullOrEmpty(ip)) {
//                eventInfo.setVisitIp(ip);
//            }
//        }

        if(analyze!=null){
            eventInfo.setVisitByChannel(analyze.getVisitByChannel());
        }
        eventInfo.setVisitIp(traceInfo.getIPv4());
        eventInfo.setRequestCostTime(Long.valueOf(requestCostTime));

        if(isFull) {
            eventInfo.setSubject(traceInfo.getSubject());
            eventInfo.setMethod(traceInfo.getMethod());
            eventInfo.setActivityScene("");
            eventInfo.setEventType(traceInfo.getEventType());
            eventInfo.setUserAgent(traceInfo.getUserAgent());
            eventInfo.setRefererUrl(traceInfo.getReferer());
            eventInfo.setActivityId(actId);
            eventInfo.setActivityType(actType);
            eventInfo.setEventValue(traceInfo.getValue());
            //新增数据
            eventInfo.setApiUri(apiUri);
            eventInfo.setEventDetail(eventDetail);
            eventInfo.setRequestBody(requestBody);
            eventInfo.setRequestResult(requestResult);
            eventInfo.setTenantId(tenantId);
        }
        return eventInfo;
    }


    public static String getString(Map<String, Object> value, String key) {
        if (value != null && value.containsKey(key)) {
            return  value.get(key).toString();
        }
        return null;

    }

    public static String getActId(Map<String, Object> value) {
        return getString(value, "actid");
    }

    public static String getActType(Map<String, Object> value) {
        return getString(value, "actsource");
    }

    public static String getScene(Map<String,Object> value){
        return getString(value,"scene");
    }
    public static String getAppId(Map<String,Object> value){
        return getString(value,"appId") == null ? getString(value,"appid") : getString(value,"appId");
    }
    public static String getUrl(Map<String,Object> value){
        return getString(value,"url");
    }

    //新增新版键值对解析

    private static String getTenantId(Map<String,Object> value) {
        return getString(value,"tenantId");
    }
    public static String getRequestUrl(Map<String,Object> value){
        return getString(value,"requestUrl");
    }

    public static String getActivityId(Map<String, Object> value) {
        return getString(value, "activityId");
    }

    public static String getActivityType(Map<String, Object> value) {
        return getString(value, "activityType");
    }

    public static String getEventDetail(Map<String,Object> value){
        return getString(value,"eventName");
    }

    public static String getApiUri(Map<String,Object> value){
        return getString(value,"api");
    }

    public static String getRequestBody(Map<String,Object> value){
        return getString(value,"body");
    }

    public static String getRequestResult(Map<String,Object> value){
        return getString(value,"result");
    }

    public static String getRequestCostTime(Map<String,Object> value){
        return getString(value,"costTime");
    }
}
