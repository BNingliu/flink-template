package com.ytd.template.logs;

import com.ytd.template.cli.SqlCommandParser;
import com.ytd.template.enums.SqlCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @date : 2022-03-15 10:12:01
 * @desc : 日志打印
 */
@Slf4j
public class LogPrint {

    /**
     * 打印SqlCommandCall 日志信息
     */
    public static void logPrint(SqlCommandParser.SqlCommandCall sqlCommandCall) {
        if (sqlCommandCall == null) {
            throw new NullPointerException("sqlCommandCall is null");
        }
        switch (sqlCommandCall.command) {
            case SET:
                System.out.println("\n############# " + sqlCommandCall.command.name() + " ############# \nSET "
                        + sqlCommandCall.operands[0] + "=" + sqlCommandCall.operands[1]);
                log.info("\n############# {} ############# \nSET{}={}", sqlCommandCall.command.name(), sqlCommandCall.operands[0], sqlCommandCall.operands[1]);
                break;
            default:
                System.out.println("\n############# " + sqlCommandCall.command.name() + " ############# \n" + sqlCommandCall.operands[0]);
                log.info("\n############# {} ############# \n {}", sqlCommandCall.command.name(), sqlCommandCall.operands[0]);
        }
    }

    /**
     * show 语句  select语句结果打印
     */
    public static void queryRestPrint(TableEnvironment tEnv, SqlCommandParser.SqlCommandCall sqlCommandCall) {
        if (sqlCommandCall == null) {
            throw new NullPointerException("sqlCommandCall is null");
        }
        LogPrint.logPrint(sqlCommandCall);


        if (sqlCommandCall.command.name().equalsIgnoreCase(SqlCommand.SELECT.name())) {
//            throw new RuntimeException("目前不支持select 语法使用");
            tEnv.executeSql(sqlCommandCall.operands[0]).print();
        } else {
            tEnv.executeSql(sqlCommandCall.operands[0]).print();
        }

//        if (sqlCommandCall.getSqlCommand().name().equalsIgnoreCase(SqlCommand.SELECT.name())) {
//            Iterator<Row> it = tEnv.executeSql(sqlCommandCall.operands[0]).collect();
//            while (it.hasNext()) {
//                String res = String.join(",", PrintUtils.rowToString(it.next()));
//                log.info("数据结果 {}", res);
//            }
//        }
    }


    /**
     * CREATE CATALOG 语句
     */
    public static void createCatalogPrint(TableEnvironment tEnv, SqlCommandParser.SqlCommandCall sqlCommandCall) {
        if (sqlCommandCall == null) {
            throw new NullPointerException("sqlCommandCall is null");
        }
        LogPrint.logPrint(sqlCommandCall);

        tEnv.executeSql(sqlCommandCall.operands[0]);

    }

}
