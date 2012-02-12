/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.TRACE;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.error;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.trace;
import static cx.ath.troja.nja.Log.warn;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.Script;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.Status;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.dhash.commands.CleanCommand;
import cx.ath.troja.chordless.dhash.commands.DHashCommand;
import cx.ath.troja.chordless.dhash.commands.ReturnValueCommand;
import cx.ath.troja.chordless.dhash.commands.SynchronizeCommand;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Describable;
import cx.ath.troja.nja.FriendlyScheduledThreadPoolExecutor;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.ThreadState;

public class DHash extends Chord {

	public static final Integer SCHEMA_VERSION = new Integer(1);

	public static final Identifier SCHEMA_VERSION_KEY = Identifier.generate(DHash.class.getName() + ".schemaVersion");

	public static final String PERSIST_EXECUTOR = "persistExecutor";

	public static final String EXEC_EXECUTOR = "execExecutor";

	public static class ErrorCommand extends ReturnValueCommand<Object> {
		public ErrorCommand(RuntimeException e) {
			super(null);
			returnValue = e;
		}

		@Override
		public void executeAway(DHash dhash) {
			throw new RuntimeException("Should never be executed");
		}

		@Override
		public void executeHome(DHash dhash) {
			throw new RuntimeException("Should never be executed");
		}
	}

	private LockingStorage storage;

	private Map<Command, Long> waitingCommandValidity;

	private Map<Command, Receiver> waitingCommands;

	private Map<Receiver, Command> waitingReceivers;

	private Identifier lastCleanedIdentifier;

	private CleanCommand lastCleanCommand;

	private long cleanTimestamp = 0;

	private long lastSynchronize = 0;

	private long synchronizeTimestamp = 0;

	private boolean runningGC = false;

	private boolean runningTransactionCleanup = false;

	private int copies = 1;

	private boolean enableClean = true;

	private boolean enableSynchronize = true;

	private boolean enableGC = true;

	private int delayFactor = 10;

	private int gcFactor = 100;

	private int initialDelay = 20;

	private ScheduledFuture cleanTask;

	private ScheduledFuture synchronizeTask;

	private ScheduledFuture gcTask;

	private ScheduledFuture waitingCommandCleaner;

	private ScheduledFuture transactionCleaner;

	private FriendlyScheduledThreadPoolExecutor persistExecutor;

	private ThreadPoolExecutor execExecutor;

	public DHash start() {
		persistExecutor = new FriendlyScheduledThreadPoolExecutor(1, new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread returnValue = new Thread(r);
				returnValue.setName("" + DHash.this.getServerInfo() + " persist executor created at " + new Date());
				ThreadState.put(returnValue, ExecutorService.class, PERSIST_EXECUTOR);
				return returnValue;
			}
		});
		execExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE, 1, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(), new ThreadFactory() {
					public Thread newThread(Runnable r) {
						Thread returnValue = new Thread(r);
						returnValue.setName("" + DHash.this.getServerInfo() + " exec executor created at " + new Date());
						ThreadState.put(returnValue, ExecutorService.class, EXEC_EXECUTOR);
						return returnValue;
					}
				});
		waitingCommands = new HashMap<Command, Receiver>();
		waitingReceivers = new HashMap<Receiver, Command>();
		waitingCommandValidity = new HashMap<Command, Long>();
		lastCleanCommand = null;
		super.start();
		persistExecutor.setTimeKeeper(getExecutorService().getTimeKeeper());
		checkSchemaVersion();
		return this;
	}

	public FriendlyScheduledThreadPoolExecutor getPersistExecutor() {
		return persistExecutor;
	}

	public ThreadPoolExecutor getExecExecutor() {
		return execExecutor;
	}

	private void checkSchemaVersion() {
		Dhasher dhasher = new Dhasher(this);
		Delay<Integer> schemaVersion = dhasher.get(SCHEMA_VERSION_KEY);
		if (schemaVersion.get() == null) {
			dhasher.put(SCHEMA_VERSION_KEY, SCHEMA_VERSION).get();
		} else if (!SCHEMA_VERSION.equals(schemaVersion.get())) {
			throw new RuntimeException("This version of DHash uses schema version " + SCHEMA_VERSION + " but the database was initialized with "
					+ schemaVersion.get() + "!");
		}
	}

	private void stop21() {
		if (cleanTask != null) {
			cleanTask.cancel(true);
		}
		if (synchronizeTask != null) {
			synchronizeTask.cancel(true);
		}
		if (gcTask != null) {
			gcTask.cancel(true);
		}
		if (waitingCommandCleaner != null) {
			waitingCommandCleaner.cancel(true);
		}
		if (transactionCleaner != null) {
			transactionCleaner.cancel(true);
		}
		persistExecutor.shutdownNow();
		persistExecutor = null;
		execExecutor.shutdownNow();
		execExecutor = null;
		storage.close();
	}

	public void stop() {
		stop1();
		stop2();
		stop21();
		stop3();
	}

	public DHash setInitialDelay(int i) {
		initialDelay = i;
		return this;
	}

	public DHash setEnableClean(boolean b) {
		enableClean = b;
		return this;
	}

	public DHash setEnableGC(boolean b) {
		enableGC = b;
		return this;
	}

	public DHash setEnableSynchronize(boolean b) {
		enableSynchronize = b;
		return this;
	}

	public boolean getEnableClean() {
		return enableClean;
	}

	public boolean getEnableGC() {
		return enableGC;
	}

	public boolean getEnableSynchronize() {
		return enableSynchronize;
	}

	public CleanCommand getLastCleanCommand() {
		return lastCleanCommand;
	}

	public void _resetCleanTimestamp() {
		cleanTimestamp = 0;
	}

	public long getCleanTimestamp() {
		return cleanTimestamp;
	}

	public DHash setGCFactor(int f) {
		gcFactor = f;
		return this;
	}

	public DHash setDelayFactor(int f) {
		delayFactor = f;
		return this;
	}

	public int getCopies() {
		return copies;
	}

	public DHash setCopies(int n) {
		if (n > N_SUCCESSORS) {
			throw new IllegalArgumentException("You can not have more copies than cached successors (which is currently " + N_SUCCESSORS + ")!");
		}
		copies = n;
		return this;
	}

	public DHash setStorage(LockingStorage s) {
		storage = s;
		return this;
	}

	public LockingStorage getStorage() {
		return storage;
	}

	@SuppressWarnings("unchecked")
	public void deliver(Command c) {
		synchronized (waitingCommands) {
			if (loggable(c, TRACE))
				trace(c, "" + c + " is delivered to " + this);
			Receiver receiver = waitingCommands.remove(c);
			waitingReceivers.remove(receiver);
			waitingCommandValidity.remove(c);
			if (receiver != null) {
				receiver.receive(c);
			}
		}
	}

	public void register(Command c, Receiver r) {
		synchronized (waitingCommands) {
			waitingCommands.put(c, r);
			waitingReceivers.put(r, c);
			waitingCommandValidity.put(c, new Long(System.currentTimeMillis() + r.getTimeout()));
		}
	}

	public void registerAndSendToSuccessor(Identifier i, DHashCommand c, Receiver r) {
		register(c, r);
		sendToSuccessor(i, c);
	}

	public boolean unregister(Receiver r) {
		synchronized (waitingCommands) {
			Command c = waitingReceivers.get(r);
			if (c == null) {
				return false;
			} else {
				return unregister(c);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public boolean unregister(Command c) {
		synchronized (waitingCommands) {
			if (waitingCommands.containsKey(c)) {
				Receiver r = waitingCommands.remove(c);
				r.receive(new ErrorCommand(new RuntimeException("Receiver unregistered")));
				waitingReceivers.remove(r);
				waitingCommandValidity.remove(c);
				return true;
			} else {
				return false;
			}
		}
	}

	public void registerAndSend(Command c, SocketAddress a, Receiver r) throws ConnectException {
		register(c, r);
		try {
			send(c, a);
		} catch (ConnectException e) {
			unregister(c);
			throw e;
		}
	}

	protected void setupTasks() {
		super.setupTasks();
		resetLastCleanedIdentifier();
		cleanTask = getPersistExecutor().scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return DHash.class.getName() + ".clean";
			}

			public void run() {
				if (DHash.this.getEnableClean()) {
					DHash.this.clean();
				}
			}
		}, initialDelay, getHaste() * delayFactor, TimeUnit.SECONDS);
		synchronizeTask = getPersistExecutor().scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return DHash.class.getName() + ".synchronize";
			}

			public void run() {
				if (DHash.this.getEnableSynchronize()) {
					DHash.this.synchronize();
				}
			}
		}, initialDelay, getHaste() * delayFactor, TimeUnit.SECONDS);
		gcTask = getPersistExecutor().scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return DHash.class.getName() + ".gc";
			}

			public void run() {
				if (DHash.this.getEnableGC()) {
					DHash.this.gc();
				}
			}
		}, initialDelay, getHaste() * gcFactor, TimeUnit.SECONDS);
		waitingCommandCleaner = getPersistExecutor().scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return DHash.class.getName() + ".cleanWaitingCommands";
			}

			public void run() {
				DHash.this.cleanWaitingCommands();
			}
		}, initialDelay, getHaste() * gcFactor, TimeUnit.SECONDS);
		transactionCleaner = getPersistExecutor().scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return DHash.class.getName() + ".cleanTransactions";
			}

			public void run() {
				DHash.this.cleanTransactions();
			}
		}, initialDelay, getHaste() * gcFactor, TimeUnit.SECONDS);
	}

	public void resetLastCleanedIdentifier() {
		lastCleanedIdentifier = getIdentifier();
		resetLastCleanCommand();
	}

	public void resetLastCleanCommand() {
		lastCleanCommand = null;
	}

	public boolean validateCleanOffer(Identifier uuid) {
		if (enableClean && lastCleanCommand != null && lastCleanCommand.getUUID().equals(uuid)) {
			cleanTimestamp = System.currentTimeMillis();
			return true;
		} else {
			return false;
		}
	}

	public boolean validateClean(CleanCommand c) {
		if (enableClean && (lastCleanCommand == c || lastCleanCommand == null) && c.getCreatedAt() > getLastTopologyChange()) {
			lastCleanCommand = c;
			cleanTimestamp = System.currentTimeMillis();
			return true;
		} else {
			return false;
		}
	}

	public boolean clean() {
		try {
			if (loggable(this, DEBUG))
				debug(this, "" + this + " running clean()");
			if (!isShutdown() && (lastCleanCommand == null || System.currentTimeMillis() - cleanTimestamp > (getHaste() * 1000 * delayFactor))) {
				lastCleanedIdentifier = storage.nextIdentifier(lastCleanedIdentifier);
				if (lastCleanedIdentifier == null) {
					lastCleanedIdentifier = getIdentifier();
				}
				sendToSuccessor(lastCleanedIdentifier, new CleanCommand(getServerInfo(), lastCleanedIdentifier));
				return true;
			}
			return false;
		} catch (Throwable e) {
			error(this, "Error while running clean", e);
			throw new RuntimeException(e);
		}
	}

	public void cleanWaitingCommands() {
		synchronized (waitingCommands) {
			try {
				for (Map.Entry<Command, Long> entry : new ArrayList<Map.Entry<Command, Long>>(waitingCommandValidity.entrySet())) {
					if (entry.getValue() < System.currentTimeMillis()) {
						unregister(entry.getKey());
					}
				}
			} catch (Throwable t) {
				error(this, "Error while running cleanWaitingCommands", t);
				throw new RuntimeException(t);
			}
		}
	}

	public void resetTransactionCleanup() {
		runningTransactionCleanup = false;
	}

	public boolean runningTransactionCleanup() {
		return runningTransactionCleanup;
	}

	public void cleanTransactions() {
		try {
			if (loggable(this, DEBUG))
				debug(this, "" + this + " running cleanTransactions()");
			if (!isShutdown() && !runningTransactionCleanup && getPredecessor() != null) {
				runningTransactionCleanup = true;
				getStorage().cleanTransactions(this);
			}
		} catch (Throwable t) {
			error(this, "Error while running cleanTransactions", t);
			throw new RuntimeException(t);
		}
	}

	public void resetGC() {
		runningGC = false;
	}

	public boolean runningGC() {
		return runningGC;
	}

	public void gc() {
		try {
			if (loggable(this, DEBUG))
				debug(this, "" + this + " running gc()");
			if (!isShutdown() && !runningGC) {
				runningGC = true;
				getStorage().gc(this);
			}
		} catch (Throwable t) {
			error(this, "Error while running gc", t);
			throw new RuntimeException(t);
		}
	}

	public boolean validateSynchronize(long validAt) {
		if (enableSynchronize && validAt == lastSynchronize && validAt > getLastTopologyChange()) {
			synchronizeTimestamp = System.currentTimeMillis();
			return true;
		} else {
			return false;
		}
	}

	public boolean synchronize() {
		try {
			if (loggable(this, DEBUG))
				debug(this, "" + this + " running synchronize()");
			if (!isShutdown() && System.currentTimeMillis() - synchronizeTimestamp > (getHaste() * 1000 * delayFactor)) {
				lastSynchronize = System.currentTimeMillis();
				ServerInfo[] successors = getSuccessorArray();
				for (int i = 0; i < copies - 1; i++) {
					if (successors[i] != null && getPredecessor() != null && !getIdentifier().equals(successors[i].getIdentifier())) {
						SynchronizeCommand command = new SynchronizeCommand(getServerInfo(), successors[i], getPredecessor().getIdentifier(),
								getIdentifier(), getStorage().getRootNode(), lastSynchronize);
						try {
							send(command, successors[i].getAddress());
						} catch (ConnectException e) {
							if (loggable(this, DEBUG))
								debug(this, "Error trying to send " + command + " to " + successors[i], e);
						}
					}
				}
				return true;
			}
			return false;
		} catch (Throwable e) {
			warn(this, "Error while running synchronize", e);
			throw new RuntimeException(e);
		}
	}

	protected Status newStatus() {
		return new DHashStatus();
	}

	protected Script createScript() {
		return new DHashScript(this);
	}

	public int getEntriesHeld() {
		return getStorage().count();
	}

	public int getEntriesOwned() {
		if (getPredecessor() == null || getPredecessor().getIdentifier().equals(getIdentifier())) {
			return getEntriesHeld();
		} else {
			return getStorage().count(getPredecessor().getIdentifier().next(), getIdentifier());
		}
	}

	@Override
	public Status getStatus() {
		return getStatus(false);
	}

	@Override
	protected Object clone(Object o) {
		return Cerealizer.unpack(Cerealizer.pack(o), getStorage().getClassLoader());
	}

	public Status getStatus(boolean withData) {
		DHashStatus returnValue = (DHashStatus) super.getStatus();
		returnValue.storageDescription = getStorage().getDescription();
		returnValue.copies = getCopies();
		if (withData) {
			returnValue.entriesHeld = getEntriesHeld();
			returnValue.entriesOwned = getEntriesOwned();
			returnValue.youngestEntryAge = getStorage().youngestEntryAge();
			returnValue.oldestEntryAge = getStorage().oldestEntryAge();
		} else {
			returnValue.entriesHeld = -1;
			returnValue.entriesOwned = -1;
			returnValue.youngestEntryAge = 0;
			returnValue.oldestEntryAge = 0;
		}
		returnValue.waitingCommands = new TreeMap<String, Integer>();
		for (Map.Entry<Command, Receiver> entry : new HashMap<Command, Receiver>(waitingCommands).entrySet()) {
			Integer now = returnValue.waitingCommands.get(entry.getValue().getClass().getName());
			if (now == null) {
				now = new Integer(0);
			}
			returnValue.waitingCommands.put(entry.getValue().getClass().getName(), now + 1);
		}
		return returnValue;
	}

}
