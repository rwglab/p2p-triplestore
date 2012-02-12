package de.rwglab.p2pts;

import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceFilter;
import de.rwglab.p2pts.util.InjectLogger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;

import javax.servlet.DispatcherType;
import java.net.InetSocketAddress;
import java.util.EnumSet;

@Singleton
public class TripleStoreRestServerService extends AbstractService {

	@InjectLogger
	private Logger log;

	private final TripleStoreConfig config;

	private final GuiceFilter guiceFilter;

	private Server server;

	@Inject
	public TripleStoreRestServerService(final TripleStoreConfig config, final GuiceFilter guiceFilter) {
		this.config = config;
		this.guiceFilter = guiceFilter;
	}

	@Override
	protected void doStart() {

		try {

			server = new Server(config.restServerAddress);

			FilterHolder guiceFilterHolder = new FilterHolder(guiceFilter);

			ServletContextHandler guiceContextHandler = new ServletContextHandler();
			guiceContextHandler.setContextPath("/");
			guiceContextHandler.addFilter(guiceFilterHolder, "/*", EnumSet.allOf(DispatcherType.class));
			guiceContextHandler.addServlet(DefaultServlet.class, "/");

			ContextHandlerCollection contexts = new ContextHandlerCollection();
			contexts.setHandlers(new Handler[]{guiceContextHandler});

			server.setHandler(contexts);
			server.start();

			log.info("Started server on {}", config.restServerAddress);

			notifyStarted();

		} catch (Exception e) {

			log.error("Failed to start server on port {} due to the following error: " + e, e);
			notifyFailed(e);
		}
	}

	@Override
	protected void doStop() {

		try {
			server.stop();
			notifyStopped();
		} catch (Exception e) {
			notifyFailed(e);
		}
	}
}
