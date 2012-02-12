package de.rwglab.p2pts;

import de.rwglab.p2pts.util.InetSocketAddressOptionHandler;
import de.rwglab.p2pts.util.Log4JLevelOptionHandler;
import org.apache.log4j.Level;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.net.InetSocketAddress;

public class TripleStoreConfig {

	@Option(name = "--brokerHost", usage = "broker hostname", required = true)
	public String brokerHost;

	@Option(name = "--brokerPort", usage = "broker port")
	public int brokerPort = 5672;

	@Option(name = "--brokerVirtualHost", usage = "broker virtual host")
	public String brokerVirtualHost = "";

	@Option(name = "--brokerUsername", usage = "broker username")
	public String brokerUsername = null;

	@Option(name = "--brokerPassword", usage = "broker password")
	public String brokerPassword = null;

	@Option(name = "--brokerExchangeName", usage = "broker exchange name", required = true)
	public String brokerExchangeName;

	@Option(name = "--brokerExchangeType", usage = "broker exchange type", required = true)
	public String brokerExchangeType;

	@Option(name = "--brokerRoutingKey", usage = "broker routing key")
	public String brokerRoutingKey;

	@Option(name = "--brokerQueueName", usage = "broker queue name", required = true)
	public String brokerQueueName;

	@Option(name = "--brokerQueueDurable", usage = "broker exchange durable")
	public boolean brokerQueueDurable;

	@Option(name = "--brokerQueueExclusive", usage = "set to make broker queue exclusive")
	public boolean brokerQueueExclusive = false;

	@Option(name = "--brokerQueueAutoDelete", usage = "set to make broker queue autodelete")
	public boolean brokerQueueAutoDelete = false;

	@Option(name = "--restServerAddress", usage = "address to start the REST server on", metaVar = "HOSTNAME:PORT",
			handler = InetSocketAddressOptionHandler.class)
	public InetSocketAddress restServerAddress;

	@Option(name = "--p2pBootstrapAddress",
			usage = "set to the hostname and port of an existing chord node to join its network",
			handler = InetSocketAddressOptionHandler.class)
	public InetSocketAddress p2pBootstrapAddress = null;

	@Option(name = "--p2pLocalAddress", usage = "the address of the local chord node",
			handler = InetSocketAddressOptionHandler.class)
	public InetSocketAddress p2pLocalAddress = null;

	@Option(name = "--p2pRDFImport", usage = "a file containing RDF data to be imported into the chord network")
	public File p2pRDFImport = null;

	@Option(name = "--rootLogLevel",
			usage = "set logging level (valid values: TRACE, DEBUG, INFO, WARN, ERROR) for the root logger (including libraries used)",
			handler = Log4JLevelOptionHandler.class)
	public Level rootLogLevel = Level.WARN;

	@Option(name = "--logLevel",
			usage = "set logging level (valid values: TRACE, DEBUG, INFO, WARN, ERROR) for project classes",
			handler = Log4JLevelOptionHandler.class)
	public Level logLevel = Level.INFO;

	@Option(name = "--verbose", usage = "verbose (DEBUG) logging output (default: INFO).")
	public boolean verbose = false;

	@Option(name = "--help", usage = "this help message.")
	public boolean help = false;
}
