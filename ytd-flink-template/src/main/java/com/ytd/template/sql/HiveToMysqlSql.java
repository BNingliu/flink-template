package com.ytd.template.sql;

import com.ytd.template.cli.CliOptions;
import com.ytd.template.cli.CliOptionsParser;
import com.ytd.template.execute.CliOptionSubmit;

/**
 * @desc hive到mysql
 */
public class HiveToMysqlSql {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hive");
//        args = new String[]{"-f  D:\\workspace-ytd\\ytd-flink-template\\src\\main\\resources\\sqlsumbit\\HiveToMysql.sql", "-p 2"};
        final CliOptions options = CliOptionsParser.parseClient(args);
        CliOptionSubmit submit = new CliOptionSubmit(options);
        submit.run(3);
    }
}
