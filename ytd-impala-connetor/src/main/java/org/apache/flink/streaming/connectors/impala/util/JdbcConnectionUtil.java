/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.impala.util;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Objects;


public class JdbcConnectionUtil {
    private static final int DEFAULT_RETRY_NUM = 3;
    private static final long DEFAULT_RETRY_TIME_WAIT = 3L;
    private static final int DEFAULT_VALID_TIME = 10;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionUtil.class);

    /**
     * 关闭连接资源
     *
     * @param rs     ResultSet
     * @param stmt   Statement
     * @param conn   Connection
     * @param commit 手动提交事务
     */
    public static void closeConnectionResource(
            ResultSet rs
            , Statement stmt
            , Connection conn
            , boolean commit) {
        if (Objects.nonNull(rs)) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", e.getMessage());
            }
        }

        if (Objects.nonNull(stmt)) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", e.getMessage());
            }
        }

        if (Objects.nonNull(conn)) {
            try {
                if (commit) {
                    commit(conn);
                } else {
                    rollBack(conn);
                }

                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", e.getMessage());
            }
        }
    }

    /**
     * 手动提交事物
     *
     * @param conn Connection
     */
    public static void commit(Connection conn) {
        try {
            if (!conn.isClosed() && conn.isValid(DEFAULT_VALID_TIME) && !conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            LOG.warn("commit error:{}", e.getMessage());
        }
    }

    /**
     * 手动回滚事物
     *
     * @param conn Connection
     */
    public static void rollBack(Connection conn) {
        try {
            if (!conn.isClosed() && conn.isValid(DEFAULT_VALID_TIME) && !conn.getAutoCommit()) {
                conn.rollback();
            }
        } catch (SQLException e) {
            LOG.warn("rollBack error:{}", e.getMessage());
        }
    }

    /**
     * get connection from datasource and retry when failed.
     *
     * @param driverName driver name for rdb datasource
     * @param url        connect url
     * @param userName   connect user name
     * @param password   password for user name
     * @return a valid connection
     */
    public static Connection getConnectionWithRetry(String driverName,
                                                    String url,
                                                    String userName,
                                                    String password) throws ClassNotFoundException {
        String message = "Get connection failed. " +
            "\nurl: [%s]" +
            "\nuserName: [%s]" +
            "\ncause: [%s]";
        String errorCause;
        String errorMessage = "";

        Class.forName(driverName,true, JdbcConnectionUtil.class.getClassLoader());
//        Preconditions.checkNotNull(url, "url can't be null!");

        for (int i = 0; i < DEFAULT_RETRY_NUM; i++) {
            try {
                Connection connection =
                    Objects.isNull(userName) ?
                        DriverManager.getConnection(url) :
                        DriverManager.getConnection(url, userName, password);
                connection.setAutoCommit(false);
                return connection;
            } catch (Exception e) {
//                errorCause = ExceptionTrace.traceOriginalCause(e);
//                errorMessage = String.format(
//                    message,
//                    url,
//                    userName,
//                    errorCause
//                );
                e.printStackTrace();
                LOG.warn(errorMessage);
                LOG.warn("Connect will retry after [{}] s. Retry time [{}] ...", DEFAULT_RETRY_TIME_WAIT, i + 1);
//                ThreadUtil.sleepSeconds(DEFAULT_RETRY_TIME_WAIT);
            }
        }
        throw new SuppressRestartsException(new SQLException(errorMessage));
    }
}
