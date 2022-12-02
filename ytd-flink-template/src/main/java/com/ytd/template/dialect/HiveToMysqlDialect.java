package com.ytd.template.dialect;

import com.ytd.template.cli.SqlCommandParser;
import org.apache.flink.api.common.JobID;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.StreamPlanner;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/03/22 10:23
 */
public class HiveToMysqlDialect extends Dialect {
    public static final String DEFAULT_CATALOG = EnvironmentSettings.DEFAULT_BUILTIN_CATALOG;

    public void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tableEnv) {
        String dml = cmdCall.operands[0];
        StreamPlanner planner = (StreamPlanner) ((TableEnvironmentImpl) tableEnv).getPlanner();
        FlinkPlannerImpl flinkPlanner = planner.createFlinkPlanner();
        RichSqlInsert sqlInsert = (RichSqlInsert) flinkPlanner.parser().parse(dml);
        StatementSet statementSet = tableEnv.createStatementSet();
        try {

            Table dataTable = tableEnv.sqlQuery(sqlInsert.getSource().toString());
            tableEnv.useCatalog(DEFAULT_CATALOG);
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

//            Table dataTable = tableEnv.sqlQuery(sqlInsert.getSource().toString());
            statementSet.addInsert(sqlInsert.getTargetTable().toString(), dataTable);
            TableResult tableResult = statementSet.execute();

            if (tableResult == null || tableResult.getJobClient().get() == null
                    || tableResult.getJobClient().get().getJobID() == null) {
                throw new RuntimeException("任务运行失败 没有获取到JobID");
            }
            JobID jobID = tableResult.getJobClient().get().getJobID();
            System.out.println(jobID);

        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }

    }

}
