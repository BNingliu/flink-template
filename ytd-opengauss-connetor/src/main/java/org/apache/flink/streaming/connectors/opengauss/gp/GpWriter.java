package org.apache.flink.streaming.connectors.opengauss.gp;

import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/11/30 14:23
 */
public class GpWriter <T> implements AutoCloseable {

    private static Connection connection = null;

    public GpWriter(JdbcConnectorOptions jdbcOptions) throws Exception {
        if (connection == null || connection.isClosed()) {
            String url = "jdbc:postgresql://192.168.2.131:5432/test";
            connection = DriverManager.getConnection(url, "tms", "123qwe");
//        }
        }

    }

    @Override
    public void close() throws Exception {
        connection.close();

    }
}
