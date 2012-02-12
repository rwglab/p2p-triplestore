/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import static cx.ath.troja.nja.Log.warn;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.chordless.gui.ControlFrame;
import cx.ath.troja.chordless.tools.LocalChordProxy;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.JDBC;
import cx.ath.troja.nja.Log;
import cx.ath.troja.nja.RuntimeArguments;

public class DHashApp {

	public static final String IDENTIFIER = "identifier";

	public static final String LOCALPORT = "localport";

	public static final String LOCALADDRESS = "localaddress";

	public static final String JDBCCLASS = "jdbcclass";

	public static final String JDBCURL = "jdbcurl";

	public static final String BOOTSTRAPADDRESS = "bootstrapaddress";

	public static final String BOOTSTRAPPORT = "bootstrapport";

	public static final String SERVICENAME = "servicename";

	public static final String COPIES = "copies";

	public static final String HEADLESS = "headless";

	public static final String DEFAULT_JDBCCLASS = "org.hsqldb.jdbcDriver";

	public static final String DEFAULT_COPIES = "3";

	public static final String DEFAULT_JDBCURL = "jdbc:hsqldb:file:dhash.db;shutdown=true";

	public static final String DEFAULT_LOCALADDRESS = "0.0.0.0";

	public static final String DEFAULT_LOCALPORT = "4545";

	public static final String LOGLEVEL = "loglevel";

	public static final String DEFAULT_LOGLEVEL = "INFO";

	public static final String DEFAULT_HEADLESS = "false";

	public static final String DEFAULT_SERVICENAME = "chordless";

	private static final Set<String> VALID_ARGUMENTS = new HashSet<String>(Arrays.asList(new String[] { COPIES, IDENTIFIER, LOCALPORT, LOCALADDRESS,
			JDBCCLASS, JDBCURL, BOOTSTRAPADDRESS, BOOTSTRAPPORT, HEADLESS, LOGLEVEL, SERVICENAME }));

	private static String minLength(String s, int l) {
		StringBuffer rval = new StringBuffer(s);
		while (rval.length() < l) {
			rval.append(" ");
		}
		return rval.toString();
	}

	private static final String HELP_MESSAGE = ("Usage: DHashApp ARGUMENTS\n" + "ARGUMENTS look like KEY=VALUE.\n" + "KEY must be one of:\n"
			+ minLength(COPIES, 20)
			+ " - the number of copies that the Chord network will try to keep of each chunk of data it contains. Will default to "
			+ DEFAULT_COPIES
			+ ".\n"
			+ minLength(IDENTIFIER, 20)
			+ " - the Chord Identifier the started node shall have. Given as a hexadecimal number. Will default to a hash of the local address and port.\n"
			+ minLength(LOCALPORT, 20)
			+ " - the local port the started node shall listen on. Given as a number. Will default to "
			+ DEFAULT_LOCALPORT
			+ ".\n"
			+ minLength(LOCALADDRESS, 20)
			+ " - the local address the started node shall listen on. Given as an ip address or hostname. Will default to "
			+ DEFAULT_LOCALADDRESS
			+ ".\n"
			+ minLength(JDBCCLASS, 20)
			+ " - the jdbc driver class the started node shall use for persistence. Given as a fully qualified class name. Will default to "
			+ DEFAULT_JDBCCLASS
			+ ".\n"
			+ minLength(JDBCURL, 20)
			+ " - the jdbc url the started node shall use for persistence. Given as a JDBC URL. Will default to "
			+ DEFAULT_JDBCURL
			+ ".\n"
			+ minLength(BOOTSTRAPADDRESS, 20)
			+ " - the address of another node whose Chord network the started node shall try to join. Given as an ip address or hostname. No default.\n"
			+ minLength(BOOTSTRAPPORT, 20)
			+ " - the port of another node whose Chord network the started node shall try to join. Given as a number. Will default to "
			+ DEFAULT_LOCALPORT
			+ ".\n"
			+ minLength(SERVICENAME, 20)
			+ " - the service name to publish (and lookup using multicast if no bootstrap address and port are given). Given as a regular string. Will default to "
			+ DEFAULT_SERVICENAME + ".\n" + minLength(HEADLESS, 20)
			+ " - whether the started node shall refrain from opening a control window. Given as 'true' or 'false'. Will default to "
			+ DEFAULT_HEADLESS + ".\n" + minLength(LOGLEVEL, 20)
			+ " - the loglevel of the java.util.loggin subsystem upon startup. Given as the name of a java.util.logging.Level. Will default to "
			+ DEFAULT_LOGLEVEL + ".");

	private static boolean goodArguments(RuntimeArguments arguments) {
		for (String argName : arguments.all().keySet()) {
			if (!VALID_ARGUMENTS.contains(argName)) {
				return false;
			}
		}
		return true;
	}

	private static void showHelp() {
		System.err.println(HELP_MESSAGE);
	}

	public static void main(String[] argv) {
		RuntimeArguments arguments = new RuntimeArguments(argv);
		if (goodArguments(arguments)) {
			Log.setLevel(Level.parse(arguments.def(LOGLEVEL, DEFAULT_LOGLEVEL)));
			new DHashApp().start(arguments);
		} else {
			showHelp();
		}
	}

	private DHash dhash;

	private RuntimeArguments arguments;

	public DHashApp() {
	}

	public DHashApp start(RuntimeArguments a) {
		arguments = a;
		initDHash();
		if (arguments.def(HEADLESS, DEFAULT_HEADLESS).equals("false")) {
			new ControlFrame(new LocalChordProxy(dhash)).setVisible(true);
		}
		return this;
	}

	public RuntimeArguments getArguments() {
		return arguments;
	}

	public DHash getDHash() {
		return dhash;
	}

	public void stop() {
		dhash.stop();
	}

	public DHashApp startDHash(InetSocketAddress local, LockingStorage storage, SocketAddress bootstrap, Identifier identifier, int copies,
			String serviceName) {
		dhash = ((DHash) new DHash().setLocal(local)).setStorage(storage).setCopies(copies);
		if (bootstrap != null) {
			dhash.setBootstrap(bootstrap);
		}
		if (identifier != null) {
			dhash.setIdentifier(identifier);
		}
		if (serviceName != null) {
			dhash.setServiceName(serviceName);
		}
		dhash.start();
		return this;
	}

	private DHashApp initDHash() {
		Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			public void uncaughtException(Thread thread, Throwable throwable) {
				warn(this, "" + thread + " threw an exception", throwable);
			}
		});
		int copies = Integer.parseInt(arguments.def(COPIES, DEFAULT_COPIES));
		LockingStorage storage = new JDBCStorage(new JDBC(arguments.def(JDBCCLASS, DEFAULT_JDBCCLASS), arguments.def(JDBCURL, DEFAULT_JDBCURL)));
		InetSocketAddress address = new InetSocketAddress(arguments.def(LOCALADDRESS, DEFAULT_LOCALADDRESS), Integer.parseInt(arguments.def(
				LOCALPORT, DEFAULT_LOCALPORT)));
		SocketAddress bootstrap = (arguments.has(BOOTSTRAPADDRESS) ? new InetSocketAddress(arguments.get(BOOTSTRAPADDRESS),
				Integer.parseInt(arguments.def(BOOTSTRAPPORT, DEFAULT_LOCALPORT))) : null);
		Identifier identifier = arguments.has(IDENTIFIER) ? new Identifier(arguments.get(IDENTIFIER)) : null;
		String serviceName = arguments.def(SERVICENAME, DEFAULT_SERVICENAME);
		startDHash(address, storage, bootstrap, identifier, copies, serviceName);
		return this;
	}

}