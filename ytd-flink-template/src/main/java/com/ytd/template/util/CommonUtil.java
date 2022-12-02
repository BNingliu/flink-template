package com.ytd.template.util;

import java.math.BigDecimal;

/**
 * @program: ytd-flink-template
 * @description: 一些通用得工具处理方法
 * @author: liuningbo
 * @create: 2022/03/17 18:01
 */
public class CommonUtil {

    /**
     *
     * @param value
     * @descript 字段去除去重
     */
    public static String repalceAndTrim(String value){
        return value.trim().replaceAll(";","");
    }


}
