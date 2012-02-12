package de.rwglab.p2pts;

import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.SingletonEventBus;
import cx.ath.troja.chordless.commands.SendToSuccessorCommand;
import cx.ath.troja.chordless.dhash.commands.GetCommand;
import cx.ath.troja.chordless.dhash.commands.PutCommand;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.TimeKeeper;
import de.rwglab.p2pts.util.InjectLogger;
import de.uniluebeck.itm.tr.util.ExecutorUtils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TripleStoreVisualizationChordObserver extends AbstractService {

	@InjectLogger
	private Logger log;

	private ScheduledExecutorService scheduler;

	private static final String REDIRECTS_KEY = SendToSuccessorCommand.class.getCanonicalName() + ".run";

	private static final String PUTS_KEY = PutCommand.class.getCanonicalName() + ".run";

	private static final String GETS_KEY = GetCommand.class.getCanonicalName() + ".run";

	private final DHashService dHashService;

	@Inject
	public TripleStoreVisualizationChordObserver(final DHashService dHashService) {
		this.dHashService = dHashService;
	}

	private Runnable vizObserver = new Runnable() {

		@Override
		public void run() {

			try {

				dHashService.getDhash().getPersistExecutor().submit(new Runnable() {

					@Override
					public void run() {

						final TripleStoreVisualizationChordStatus status = new TripleStoreVisualizationChordStatus();

						ServerInfo predecessor = dHashService.getDhash().getPredecessor();
						final Identifier to = dHashService.getDhash().getIdentifier();
						final Identifier from = predecessor == null ? to : predecessor.getIdentifier().next();

						status.min = from.getValue();
						status.max = to.getValue();
						status.id = to.getValue();
						status.hostname = ((InetSocketAddress) dHashService.getDhash().getServerInfo().getAddress())
								.getHostName();
						status.triples = dHashService.getDhash().getStorage().count(from, to);

						Map<String, TimeKeeper.Occurence> occurrences =
								dHashService.getDhash().getStatus().executorTimer.getCounts();

						status.redirects =
								occurrences.get(REDIRECTS_KEY) == null ? 0 : occurrences.get(REDIRECTS_KEY).times();
						status.gets = occurrences.get(GETS_KEY) == null ? 0 : occurrences.get(GETS_KEY).times();
						status.puts = occurrences.get(PUTS_KEY) == null ? 0 : occurrences.get(PUTS_KEY).times();

						SingletonEventBus.getEventBus().post(status);
					}
				}
				).get();

			} catch (InterruptedException e) {
				log.debug("", e);
			} catch (ExecutionException e) {
				log.debug("", e);
			}
		}
	};

	@Override
	protected void doStart() {

		try {

			this.scheduler = Executors.newScheduledThreadPool(3);
			this.scheduler.scheduleAtFixedRate(vizObserver, 5, 5, TimeUnit.SECONDS);

			notifyStarted();

		} catch (Exception e) {
			notifyFailed(e);
		}
	}

	@Override
	protected void doStop() {

		try {

			ExecutorUtils.shutdown(this.scheduler, 10, TimeUnit.SECONDS);

			notifyStopped();

		} catch (Exception e) {
			notifyFailed(e);
		}
	}
}
