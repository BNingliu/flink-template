package org.apache.flink.streaming.connectors.greenplum.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);


    private DBUtil() {
    }


    static final int SOCKET_TIMEOUT_INSECOND = 172800;

    public static Connection getConnection(
            final String jdbcUrl,
            final String username,
            final String password,
            final String driverName
    ) {
        Properties prop = new Properties();
        prop.put("user", username);
        prop.put("password", password);
        try {
//            Class.forName("org.postgresql.Driver");
            DriverManager.setLoginTimeout(TIMEOUT_SECONDS);
            return DriverManager.getConnection(jdbcUrl, prop);
        } catch (Exception e) {
            throw new FlinkRuntimeException("DriverManager error",e);
        }

//        return getConnection( jdbcUrl, username, password, String.valueOf(SOCKET_TIMEOUT_INSECOND * 1000));
    }


//    public static Connection getConnection(
//            final String jdbcUrl,
//            final String username,
//            final String password,
//            final String socketTimeout
//    ) {
//
//        try {
//            return RetryUtil.executeWithRetry(new Callable<Connection>() {
//                @Override
//                public Connection call() throws Exception {
//                    return DBUtil.connect( jdbcUrl, username,
//                            password, socketTimeout);
//                }
//            }, 9, 1000L, true);
//        } catch (Exception e) {
//            throw new FlinkRuntimeException(
//                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
//        }
//    }



    private static synchronized Connection connect(String url, String user, String pass, String socketTimeout) {

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);
        return connect(url, prop);
    }
    static final int TIMEOUT_SECONDS = 15;

    private static synchronized Connection connect( String url, Properties prop) {
        try {

            Class.forName("org.postgresql.Driver");
            DriverManager.setLoginTimeout(TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, SOCKET_TIMEOUT_INSECOND);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn         Database connection .
     * @param sql          sql statement to be executed
     * @param fetchSize
     * @param queryTimeout unit:second
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return query(stmt, sql);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param stmt {@link Statement}
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this
     * {@link ResultSet}
     *
     * @param rs {@link ResultSet} to be closed
     * @throws IllegalArgumentException
     */
    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                    stmt = null;
                }
                rs.close();
            }
            rs = null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }



    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        //默认3600 seconds
        stmt.setQueryTimeout(SOCKET_TIMEOUT_INSECOND);
        return query(stmt, sql);
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
                new ArrayList<String>(), new ArrayList<Integer>(),
                new ArrayList<String>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;

        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }



    public static String getCopySql(String tableName, List<String> columnList, int segment_reject_limit) {
        StringBuilder sb = new StringBuilder().append("COPY ").append(tableName).append("(")
                .append(constructColumnNameList(columnList))
                .append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");
        if (segment_reject_limit >= 2) {
            sb.append(" LOG ERRORS SEGMENT REJECT LIMIT ").append(segment_reject_limit).append(";");
        } else {
            sb.append(";");
        }
        String sql = sb.toString();
        return sql;
    }

    public static String constructColumnNameList(List<String> columnList) {
        List<String> columns = new ArrayList<String>();

        for (String column : columnList) {
            if (column.endsWith("\"") && column.startsWith("\"")) {
                columns.add(column);
            } else {
                columns.add("\"" + column + "\"");
            }
        }

        return StringUtils.join(columns, ",");
    }
}
