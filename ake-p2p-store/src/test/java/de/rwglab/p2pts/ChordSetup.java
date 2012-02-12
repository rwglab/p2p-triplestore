package de.rwglab.p2pts;

import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
import cx.ath.troja.nja.JDBC;
import de.uniluebeck.itm.tr.util.ExecutorUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ChordSetup {

	public final static String JDBC_DRIVER = "org.hsqldb.jdbcDriver";

	public final static String JDBC_URL_1 = "jdbc:hsqldb:mem:test1";

	public final static String JDBC_URL_2 = "jdbc:hsqldb:mem:test2";

	public final static String JDBC_URL_3 = "jdbc:hsqldb:mem:test3";

	private ExecutorService executor;

	private DHashService bootstrapDHashService;

	private DHashService joiningDHashService1;

	private DHashService joiningDHashService2;

	@Before
	public void setUp() throws Exception {

		final InetSocketAddress bootstrapAddress = getFreeLocalInetAddress();
		final String serviceName = new Random().nextInt(Integer.MAX_VALUE) + "";

		new JDBCStorage(new JDBC(JDBC_DRIVER, JDBC_URL_1)).destroy();
		new JDBCStorage(new JDBC(JDBC_DRIVER, JDBC_URL_2)).destroy();
		new JDBCStorage(new JDBC(JDBC_DRIVER, JDBC_URL_3)).destroy();

		bootstrapDHashService = new DHashService(serviceName, bootstrapAddress, null, JDBC_DRIVER, JDBC_URL_1);
		joiningDHashService1 =
				new DHashService(serviceName, getFreeLocalInetAddress(), bootstrapAddress, JDBC_DRIVER, JDBC_URL_2);
		joiningDHashService2 =
				new DHashService(serviceName, getFreeLocalInetAddress(), bootstrapAddress, JDBC_DRIVER, JDBC_URL_3);

		bootstrapDHashService.start().get();
		joiningDHashService1.start().get();
		joiningDHashService2.start().get();

		executor = Executors.newCachedThreadPool();
	}

	@After
	public void tearDown() throws Exception {

		bootstrapDHashService.stop().get();
		joiningDHashService1.stop().get();
		joiningDHashService2.stop().get();

		ExecutorUtils.shutdown(executor, 0, TimeUnit.SECONDS);
	}

	public InetSocketAddress getFreeLocalInetAddress() {
		try {

			ServerSocket server = new ServerSocket(0);
			SocketAddress address = new InetSocketAddress(
					InetAddress.getLocalHost().getHostName(),
					server.getLocalPort()
			);
			server.close();
			return (InetSocketAddress) address;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public DHashService getBootstrapDHashService() {
		return bootstrapDHashService;
	}

	public ExecutorService getExecutor() {
		return executor;
	}
}
