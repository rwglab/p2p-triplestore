package de.rwglab.p2pts;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Dhasher;
import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
import cx.ath.troja.nja.JDBC;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DHashService extends AbstractService {

	private final InetSocketAddress localAddress;

	private final String jdbcDriver;

	private final String jdbcUrl;

	private final String serviceName;

	@Nullable
	private final InetSocketAddress bootstrapAddress;

	private DHash dhash;

	private Dhasher dhasher;

	public DHashService(final String serviceName,
						final InetSocketAddress localAddress,
						@Nullable final InetSocketAddress bootstrapAddress,
						final String jdbcDriver,
						final String jdbcUrl) {

		this.serviceName = checkNotNull(serviceName);
		this.bootstrapAddress = bootstrapAddress;
		this.localAddress = checkNotNull(localAddress);
		this.jdbcDriver = checkNotNull(jdbcDriver);
		this.jdbcUrl = checkNotNull(jdbcUrl);
	}

	@Override
	protected void doStart() {

		try {

			JDBCStorage storage = new JDBCStorage(new JDBC(jdbcDriver, jdbcUrl));

			dhash = ((DHash) new DHash()
					.setServiceName(serviceName)
					.setLocal(localAddress)
					.setBootstrap(bootstrapAddress))
					.setDelayFactor(1)
					.setInitialDelay(1)
					.setStorage(storage);

			dhash.clean();
			dhash.start();

			if (bootstrapAddress != null) {
				while (dhash.getPredecessor() == null) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						notifyFailed(e);
					}
				}
			}

			dhasher = new Dhasher(dhash);

			notifyStarted();

		} catch (Exception e) {
			notifyFailed(e);
		}
	}

	@Override
	protected void doStop() {

		try {

			dhash.stop();
			notifyStopped();

		} catch (Exception e) {
			notifyFailed(e);
		}
	}

	public boolean isConnected() {
		return !dhash.isShutdown();
	}

	public DHash getDhash() {
		checkState(isConnected());
		return dhash;
	}

	public Dhasher getDhasher() {
		checkState(isConnected());
		return dhasher;
	}

	@VisibleForTesting
	public void clearLocalStorage() {
		new JDBCStorage(new JDBC(jdbcDriver, jdbcUrl)).destroy();
	}
}
