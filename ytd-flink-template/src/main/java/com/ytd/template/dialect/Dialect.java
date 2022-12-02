package com.ytd.template.dialect;

import com.ytd.template.cli.SqlCommandParser;
import com.ytd.template.enums.SqlCommand;
import com.ytd.template.logs.LogPrint;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import java.util.Optional;

public abstract class Dialect{

    public void createCatalogPrint(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tEnv) {
        if (cmdCall == null) {
            throw new NullPointerException("sqlCommandCall is null");
        }
        LogPrint.logPrint(cmdCall);
        tEnv.executeSql(cmdCall.operands[0]);
    }


    public void callSet(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tEnv) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return;
        }
        if (TableConfigOptions.TABLE_SQL_DIALECT.key().equalsIgnoreCase(key.trim())
                && SqlDialect.HIVE.name().equalsIgnoreCase(value.trim())) {
            tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        } else if (TableConfigOptions.TABLE_SQL_DIALECT.key().equalsIgnoreCase(key.trim())
                && SqlDialect.DEFAULT.name().equalsIgnoreCase(value.trim())) {
            tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        } else {
            Configuration configuration = tEnv.getConfig().getConfiguration();
            configuration.setString(key, value);
        }

    }

    public void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall,StreamTableEnvironment tEnv) {
        String dml = cmdCall.operands[0];
        Optional<JobClient> jobClient;
        try {
            System.out.println(dml);
            TableResult result = tEnv.executeSql(dml);
            jobClient = result.getJobClient();
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }

        if (jobClient.isPresent()) {
            JobID jobID = jobClient.get().getJobID();
            System.out.println("任务提交成功,JobId: " + jobID);
        }

    }

    public void callSelect(SqlCommandParser.SqlCommandCall cmdCall,StreamTableEnvironment tEnv) {
        String dml = cmdCall.operands[0];
        if (cmdCall.command.name().equalsIgnoreCase(SqlCommand.SELECT.name())) {
//            throw new RuntimeException("目前不支持select 语法使用");
            tEnv.executeSql(dml).print();
        } else {
            tEnv.executeSql(dml).print();
        }

    }


    public  void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tEnv) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
        String tableName = ddl.split("\\s+")[2];
        System.out.println("创建表 " + tableName + " 成功");

    }

    public  void userCatalogPrint(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tEnv){
        String dml = cmdCall.operands[0];
        LogPrint.logPrint(cmdCall);
        tEnv.executeSql(dml);

    }
}

