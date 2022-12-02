package com.ytd.template.execute;

import com.ytd.template.cli.CliOptions;
import com.ytd.template.cli.SqlCommandParser;
import com.ytd.template.dialect.CallCommand;
import com.ytd.template.dialect.Dialect;
import com.ytd.template.dialect.Platform;
import com.ytd.template.util.HdfsUtil;
import lombok.Getter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: tms-flink
 * @description:
 * @author: liuningbo
 * @create: 2022/03/18 14:58
 */
@Getter
public class CliOptionSubmit {

    private Integer parallelism;
    private String sqlFilePath;
    private Integer pathMode;
    private StreamTableEnvironment tEnv;

    public CliOptionSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.parallelism = Integer.valueOf(options.getParallelism().trim());
        this.pathMode = Integer.valueOf(options.getPathMode().trim());
    }

    public void run(Integer type) throws Exception {
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        environment.setParallelism(this.parallelism);

        this.tEnv = StreamTableEnvironment.create(environment);

        // environment.enableCheckpointing(60000);  //头和头
        List<String> sqls = new ArrayList<>();
//        //本地文件测试可用
        if (pathMode == 1) {
            sqls = Files.readAllLines(Paths.get(sqlFilePath.trim()));
        } else {
            //hdfs模式
            HdfsUtil hdfsUtil = new HdfsUtil(false);
            sqls = hdfsUtil.getFileToString(sqlFilePath.trim());
        }
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sqls);

        if (calls.size() == 0) {
            //no sql to execute
            throw new RuntimeException("There is no sql statement to execute,please check your sql file: " + sqlFilePath);
        }

        Dialect dialect = Platform.getPlatform(type).getDialect();
        for (SqlCommandParser.SqlCommandCall call : calls) {
            CallCommand.getCallCommand(call, dialect, tEnv);
        }
    }

}
