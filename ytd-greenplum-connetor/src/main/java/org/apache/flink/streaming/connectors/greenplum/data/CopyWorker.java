package org.apache.flink.streaming.connectors.greenplum.data;

import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.connectors.greenplum.util.DBUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.util.List;

public class CopyWorker {
    private static final Logger LOG = LoggerFactory.getLogger(CopyWorker.class);

    private Connection connection;
    private String sql = null;

    public CopyWorker(Connection conn, String copySql) throws IOException {
        this.connection = conn;
        this.sql = copySql;
    }

    public void copyData(List<byte[]> dataQueue, JdbcConnectorOptions connectorOptions) {
        try (
                PipedOutputStream pipeOut = new PipedOutputStream();
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(pipeOut, 102400);
                PipedInputStream pipeInt = new PipedInputStream(pipeOut, 102400)
        ) {

            for (byte[] bytes : dataQueue) {
                bufferedOutputStream.write(bytes);
            }
            pipeOut.flush();
            bufferedOutputStream.flush();
            bufferedOutputStream.close();
            pipeOut.close();

//            String result = new BufferedReader(new InputStreamReader(pipeInt))
//                    .lines().collect(Collectors.joining(System.lineSeparator()));
//            System.out.println(result);
            if(connection==null||connection.isClosed()){
                connection =  DBUtil.getConnection(
                        connectorOptions.getDbURL(),
                        connectorOptions.getUsername().get(),
                        connectorOptions.getPassword().get(),
                        connectorOptions.getDriverName()
                );
            }
            LOG.info("copy start ...");
            CopyManager mgr = new CopyManager((BaseConnection) connection);
            mgr.copyIn(sql, pipeInt);
            LOG.info("copy end ...");
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new FlinkRuntimeException("copy data", e);
        }
    }


}
