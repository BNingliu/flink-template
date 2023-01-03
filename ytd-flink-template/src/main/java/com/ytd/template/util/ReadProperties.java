package com.ytd.template.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/08/01 09:48
 */
public class ReadProperties {

    private static Properties properties = new Properties();
    private static String filePath= "config-%s.properties";
    private static String fileDefaultPath= "config-demo.properties";

    public static String getProperty(String keyWord,String active) {
        // 使用ClassLoader加载properties配置文件生成对应的输入流
        String format = String.format(filePath, active);
        try (InputStream in = ReadProperties.class.getClassLoader().getResourceAsStream(format)){
            //使用properties对象加载输入流
            properties.load(in);
            //获取key对应的value值
            return properties.getProperty(keyWord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String getProperty(String keyWord) {
        // 使用ClassLoader加载properties配置文件生成对应的输入流
        try (InputStream in = ReadProperties.class.getClassLoader().getResourceAsStream(fileDefaultPath)){
            //使用properties对象加载输入流
            properties.load(in);
            //获取key对应的value值
            return properties.getProperty(keyWord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
//        System.out.println(getProperty("hdfs.host"));
    }

}
