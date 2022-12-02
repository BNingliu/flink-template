package com.ytd.template.util;

import java.sql.*;

public class MysqlJdbcUtil {

    private static String driver = "com.mysql.jdbc.Driver";//驱动名称
    private static String url = "jdbc:mysql://192.168.2.131:3306/data_integration?&useSSL=false&serverTimezone=UTC";//链接地址，其中databasedemo是自己创建数据库，需要修改
    private static String user = "root";//用户名
    private static String password = "ytdinfo123"; // 密码，自己填
    static  Connection connection;
    static {
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    //私有化构造方法
    private MysqlJdbcUtil(){}

    //提供公共的访问方式
    public static Connection getConnection(){
        return connection;
    }
}
