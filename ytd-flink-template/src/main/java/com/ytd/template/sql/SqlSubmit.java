package com.ytd.template.sql;

import com.ytd.template.cli.CliOptions;
import com.ytd.template.cli.CliOptionsParser;
import com.ytd.template.execute.CliOptionSubmit;

/**
 * @date : 2022-01-15 10:12:01
 * @desc : 提交SQL的命令
 */
public class SqlSubmit {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hive");
//        args = new String[]{"-f  D:\\workspace-ytd\\ytd-flink-template\\src\\main\\resources\\sqlsumbit\\KafkaToHive.sql", "-p 2"};
        final CliOptions options = CliOptionsParser.parseClient(args);
        CliOptionSubmit submit = new CliOptionSubmit(options);
        submit.run(1);
    }

}
