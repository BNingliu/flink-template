package com.ytd.template.util.event;

import com.alibaba.fastjson.JSONObject;
import com.ytd.template.bean.TraceInfo;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by aaron.pan on 2017/3/22.
 */
public class TraceAnalyze {
    TraceInfo _info;

    public TraceAnalyze(TraceInfo info) {

        _info = info;

        if (_info == null) {
            _info = new TraceInfo();
        }

    }

    public String getUserId() {
        String userId = _info.getUserId();
        if (!StringUtils.isEmpty(userId)) {
            return userId;
        }

        String cookieJson = _info.getCookie();
        if (!StringUtils.isEmpty(cookieJson)) {
            JSONObject cookieObject = JSONObject.parseObject(cookieJson);
            String openId = "";
            if (cookieObject.containsKey("cookie_open_id")) {
                openId = cookieObject.getString("cookie_open_id");
            }
            if (StringUtils.isEmpty(openId) && cookieObject.containsKey("JSESSIONID")) {
                openId = cookieObject.getString("JSESSIONID");
            }
            return openId;
        }

        return null;
    }

    public String getSkin(){
        String cookieJson = _info.getCookie();
        if (!StringUtils.isEmpty(cookieJson)) {
            JSONObject cookieObject = JSONObject.parseObject(cookieJson);
            String skin = "未知";
            if (cookieObject.containsKey("mulri_skin_id")) {
                skin = cookieObject.getString("mulri_skin_id");
            }
            return skin;
        }

        return "未知";
    }

    public String getVisitByChannel(){
        String queryString=_info.getQueryString();
        String value="";
        if(!StringUtils.isEmpty(queryString)){
            String[] qs=queryString.split("&");
            for(String s:qs){
                if(!StringUtils.isEmpty(s)){
                    String[] kvs=s.split("=");
                    if(kvs.length==2&&kvs[0].toLowerCase().equals("from")){
                        value = kvs[1];
                    }
                }
            }
        }
        return value;
    }

//    public ClientInfo getClientInfo() {
//
//        ClientInfo clientInfo = new ClientInfo();
//
//        String userAgentJson = _info.getUserAgent();
//        if (!StringUtils.isEmpty(userAgentJson)) {
//            UserAgent userAgent = new UserAgent(userAgentJson);
//            if (userAgent != null) {
//                Browser browser = userAgent.getBrowser();
//                if (browser != null) {
//                    clientInfo.setBrowser(browser.getName());
//                }
//                Version version = userAgent.getBrowserVersion();
//                if (version != null) {
//                    clientInfo.setBrowserVersion(version.getVersion());
//                }
//                OperatingSystem system = userAgent.getOperatingSystem();
//                if (system != null) {
//                    clientInfo.setOs(system.getName());
//                }
//            }
//        }
//
//        clientInfo.setClientIP(_info.getiPv4());
//        clientInfo.setClientPort(_info.getPort());
//        clientInfo.setCreateTime(new Date());
//        clientInfo.setEvent(_info.getEvent());
//        clientInfo.setEventType(_info.getEventType());
//        clientInfo.setProjectId(_info.getProjectId());
//        clientInfo.setSubject(_info.getSubject());
//        clientInfo.setUrl(_info.getUrl());
//        clientInfo.setUserId(getUserId());
//        clientInfo.setController("");
//        clientInfo.setAction("");
//        clientInfo.setSkin(getSkin());
//        return clientInfo;
//
//    }
}
