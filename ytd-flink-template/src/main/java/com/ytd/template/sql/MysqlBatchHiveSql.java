package com.ytd.template.sql;

import com.ytd.template.cli.CliOptions;
import com.ytd.template.cli.CliOptionsParser;
import com.ytd.template.execute.CliOptionSubmit;

/**
 * @desc mysql单表jdbc同步到hive
 */
public class MysqlBatchHiveSql {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hive");
//        args = new String[]{"-f  D:\\workspace\\tms-flink\\ytd-example\\src\\main\\resources\\sqlsumbit\\MysqlToHive.sql", "-p 2"};
        final CliOptions options = CliOptionsParser.parseClient(args);
        CliOptionSubmit submit = new CliOptionSubmit(options);
        submit.run(2);
    }
}
