package org.apache.flink.streaming.connectors.opengauss.gp;

import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.opengauss.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.*;

public class CopyWorker implements Callable<Long> {
	private static final Logger LOG = LoggerFactory.getLogger(CopyWorker.class);

	private Connection connection;
	private LinkedBlockingQueue<byte[]> queue = null;
	private FutureTask<Long> copyResult = null;
	private String sql = null;
	private PipedInputStream pipeIn = null;
	private PipedOutputStream pipeOut = null;
	private Thread copyBackendThread = null;

	public CopyWorker(String copySql, LinkedBlockingQueue<byte[]> queue) throws IOException {
		this.queue = queue;
		this.pipeOut = new PipedOutputStream();
		this.pipeIn = new PipedInputStream(pipeOut);
		this.sql = copySql;


		this.copyResult = new FutureTask<Long>(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				try {
					CopyManager mgr = new CopyManager((BaseConnection) connection);
					return mgr.copyIn(sql, pipeIn);
				} finally {
					try {
						pipeIn.close();
					} catch (Exception ignore) {
					}
				}
			}
		});

		copyBackendThread = new Thread(copyResult);
		copyBackendThread.setName(sql);
		copyBackendThread.setDaemon(true);
		copyBackendThread.start();
	}

	@Override
	public Long call() throws Exception {
		Thread.currentThread().setName("CopyWorker");

		byte[] data = null;
		try {
			while (true) {
				data = queue.poll(1000L, TimeUnit.MILLISECONDS);
//				if (data == null && false == task.moreData()) {

				if (data == null ) {
					break;
				} else if (data == null) {
					continue;
				}

				pipeOut.write(data);
			}

			pipeOut.flush();
			pipeOut.close();
		} catch (Exception e) {
			try {
				((BaseConnection) connection).cancelQuery();
			} catch (SQLException ignore) {
				// ignore if failed to cancel query
			}

			try {
				copyBackendThread.interrupt();
			} catch (SecurityException ignore) {
			}

			try {
				copyResult.get();
			} catch (ExecutionException exec) {
				if (exec.getCause() instanceof PSQLException) {
					e.printStackTrace();
				}
				// ignore others
			} catch (Exception ignore) {
			}

			e.printStackTrace();
		} finally {
			try {
				pipeOut.close();
			} catch (Exception e) {
				// ignore if failed to close pipe
			}

			try {
				copyBackendThread.join(0);
			} catch (Exception e) {
				// ignore if thread is interrupted
			}

//			DBUtil.closeDBResources(null, null, connection);
		}

		try {
			Long count = copyResult.get();
			return count;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}



}
