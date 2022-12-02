package com.ytd.template.dialect;

import com.ytd.template.cli.SqlCommandParser;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/03/18 14:51
 */
public class CallCommand {

    public static void getCallCommand(SqlCommandParser.SqlCommandCall cmdCall, Dialect dialect, StreamTableEnvironment tEnv ) {
        switch (cmdCall.command) {
            case SET:
                dialect.callSet(cmdCall,tEnv);
                break;
            case SELECT:
            case SHOW_CATALOGS:
            case SHOW_DATABASES:
            case SHOW_MODULES:
            case SHOW_TABLES:
                dialect.callSelect(cmdCall,tEnv);
                break;
            case CREATE_TABLE:
                dialect.callCreateTable(cmdCall,tEnv);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                dialect.callInsertInto(cmdCall,tEnv);
                break;
            case CREATE_CATALOG:
                dialect.createCatalogPrint(cmdCall,tEnv);
                break;
            case USE_CATALOG:
            case DROP_TABLE:
                dialect.userCatalogPrint(cmdCall,tEnv);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }


}
