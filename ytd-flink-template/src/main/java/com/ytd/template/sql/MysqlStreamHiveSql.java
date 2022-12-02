package com.ytd.template.sql;

import com.ytd.template.cli.CliOptions;
import com.ytd.template.cli.CliOptionsParser;
import com.ytd.template.execute.CliOptionSubmit;
import com.ytd.template.util.ReadProperties;

/**
 * @desc 单表jdbc同步
 */
public class MysqlStreamHiveSql {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", ReadProperties.getProperty("hdfs.username"));
//        args = new String[]{"-f  D:\\workspace\\tms-flink\\ytd-example\\src\\main\\resources\\sqlsumbit\\MysqlToHive.sql", "-p 1"};
        final CliOptions options = CliOptionsParser.parseClient(args);
        CliOptionSubmit submit = new CliOptionSubmit(options);
        submit.run(4);
    }
}
