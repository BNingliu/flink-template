package com.ytd.template.dialect;

import com.ytd.template.cli.SqlCommandParser;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.types.Row;

/**
 * @program: ytd-flink-template
 * @description: 默认
 * @author: liuningbo
 * @create: 2022/03/18 14:48
 */
public class MysqlStreamToHiveDialect extends Dialect {

    public static final String DEFAULT_CATALOG = EnvironmentSettings.DEFAULT_BUILTIN_CATALOG;
    public static final String DEFAULT_DATABASE = EnvironmentSettings.DEFAULT_BUILTIN_DATABASE;

    public void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tableEnv) {
        String dml = cmdCall.operands[0];

        StreamPlanner planner = (StreamPlanner) ((TableEnvironmentImpl) tableEnv).getPlanner();
        FlinkPlannerImpl flinkPlanner = planner.createFlinkPlanner();
        RichSqlInsert sqlInsert = (RichSqlInsert) flinkPlanner.parser().parse(dml);
//        SqlSelect sqlSelect = (SqlSelect)sqlInsert.getSource();

        String hiveCatalog = tableEnv.getCurrentCatalog();
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        //切换catalog
        tableEnv.useCatalog(DEFAULT_CATALOG);

        Table dataTable = tableEnv.sqlQuery(sqlInsert.getSource().toString());
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(dataTable,Row.class);
//        DataStream<Row> aTrue = tableEnv.toRetractStream(dataTable, Row.class).filter(f -> {
//            return f.f0.toString().equals("true");
//        }).map(m -> m.f1);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        //数据查询
        tableEnv.useCatalog(hiveCatalog);

        String viewName ="view_"+ sqlInsert.getTargetTable().toString();

        tableEnv.createTemporaryView(viewName, retractStream);

        try {
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

            dataTable.printSchema();
            tableEnv.from(viewName).printSchema();
            StringBuilder inserSql = new StringBuilder();
            inserSql
                    .append("insert ")
                    .append(sqlInsert.isOverwrite()?" overwrite ":" into ")
                    .append(sqlInsert.getTargetTable().toString())
                    .append(" select f1.*  from ")
                    .append(viewName);

            tableEnv.executeSql(inserSql.toString());

        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }

    }

}
