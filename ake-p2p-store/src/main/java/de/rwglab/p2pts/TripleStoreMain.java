package de.rwglab.p2pts;

import com.google.inject.Guice;
import com.google.inject.Injector;
import cx.ath.troja.nja.Log;
import de.uniluebeck.itm.tr.util.Logging;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.LogManager;

public class TripleStoreMain {

	static {

		Logging.setLoggingDefaults(Level.TRACE);
		Log.setLevel(java.util.logging.Level.FINE);

		// Jersey uses java.util.logging - bridge to slf4
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		Handler[] handlers = rootLogger.getHandlers();
		for (final Handler handler : handlers) {
			rootLogger.removeHandler(handler);
		}
		SLF4JBridgeHandler.install();
	}

	public final static String JDBC_DRIVER = "org.hsqldb.jdbcDriver";

	public final static String JDBC_URL_PREFIX = "jdbc:hsqldb:mem:test";

	public static void main(final String[] args) throws ExecutionException, InterruptedException {

		final TripleStoreConfig config = parseCmdLineOptions(args);

		setLogLevel(config);

		final DHashService dHashService = new DHashService(
				"TripleStore",
				config.p2pLocalAddress,
				config.p2pBootstrapAddress,
				JDBC_DRIVER,
				JDBC_URL_PREFIX + config.p2pLocalAddress.getHostName() + ":" + config.p2pLocalAddress.getPort()
		);

		dHashService.start().get();

		final Executor executor = Executors.newCachedThreadPool();
		final SetMapAsync<String, String> setMapAsync =
				new ChordBasedSetMapAsync<String, String>(dHashService.getDhasher(), executor);

		final TripleStoreImpl tripleStore = new TripleStoreImpl(setMapAsync, executor);

		Injector injector = Guice.createInjector(new TripleStoreModule(config, tripleStore, dHashService));

		if (config.p2pRDFImport != null) {
			Executors.newSingleThreadExecutor().execute(new Runnable() {
				@Override
				public void run() {
					new TripleStoreRDFImporter(tripleStore).importRDF(config.p2pRDFImport);
				}
			}
			);
		}

		RabbitService rabbitService = injector.getInstance(RabbitService.class);
		TripleStoreVisualizationChordObserver vizObserverChordObserver =
				injector.getInstance(TripleStoreVisualizationChordObserver.class);
		TripleStoreRestServerService tripleStoreRestServerService =
				injector.getInstance(TripleStoreRestServerService.class);

		try {

			rabbitService.start().get();
			vizObserverChordObserver.start().get();
			tripleStoreRestServerService.start().get();

		} catch (InterruptedException e) {
			LoggerFactory.getLogger(TripleStoreMain.class).error("", e);
			System.exit(1);
		} catch (ExecutionException e) {
			LoggerFactory.getLogger(TripleStoreMain.class).error("", e);
			System.exit(1);
		}
	}

	private static void setLogLevel(final TripleStoreConfig config) {

		if (config.verbose) {
			Logger.getRootLogger().setLevel(Level.DEBUG);
		} else {
			Logger.getRootLogger().setLevel(config.rootLogLevel);
			Logger.getLogger("de.rwglab.p2pts").setLevel(config.logLevel);
		}
	}

	private static TripleStoreConfig parseCmdLineOptions(final String[] args) {

		TripleStoreConfig options = new TripleStoreConfig();
		CmdLineParser parser = new CmdLineParser(options);

		try {
			parser.parseArgument(args);
			if (options.help) {
				printHelpAndExit(parser);
			}
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			printHelpAndExit(parser);
		}

		return options;
	}

	private static void printHelpAndExit(CmdLineParser parser) {
		System.err.print("Usage: java " + TripleStoreMain.class.getCanonicalName());
		parser.printSingleLineUsage(System.err);
		System.err.println();
		parser.printUsage(System.err);
		System.exit(1);
	}

}
