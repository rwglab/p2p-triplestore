/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.storage;

import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.error;
import static cx.ath.troja.nja.Log.warn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.commands.CommuteCommand;
import cx.ath.troja.chordless.dhash.commands.EntryOfferCommand;
import cx.ath.troja.chordless.dhash.commands.ExecCommand;
import cx.ath.troja.chordless.dhash.transactions.TransactionBackend;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.ReferenceMap;

public abstract class LockingStorage extends ExecStorage {

	public static class LockResponse implements Serializable {
		public static final int LOCK_SUCCESS = 0;

		public static final int LOCK_ALREADY_LOCKED = 1;

		public static final int LOCK_OUTDATED = 2;

		public int code;

		public long version;

		public long commutation;

		public static LockResponse success() {
			LockResponse returnValue = new LockResponse();
			returnValue.code = LOCK_SUCCESS;
			return returnValue;
		}

		public static LockResponse failure() {
			LockResponse returnValue = new LockResponse();
			returnValue.code = LOCK_ALREADY_LOCKED;
			return returnValue;
		}

		private LockResponse() {
			version = -1;
			commutation = -1;
		}

		public LockResponse(long v, long com) {
			code = LOCK_OUTDATED;
			version = v;
			commutation = com;
		}

		public String codeString() {
			switch (code) {
			case LOCK_SUCCESS:
				return "SUCCESS";
			case LOCK_ALREADY_LOCKED:
				return "FAILED TO LOCK, ALREADY LOCKED";
			case LOCK_OUTDATED:
				return "FAILED TO LOCK, WRONG VERSION OR COMMUTATION";
			default:
				throw new RuntimeException("Unknown " + this.getClass().getName() + " code: " + code);
			}
		}

		public String extraString() {
			Collection<String> returnValue = new ArrayList<String>();
			if (version != -1) {
				returnValue.add("entry in version " + version);
			}
			if (commutation != -1) {
				returnValue.add("entry in commutation " + commutation);
			}
			return returnValue.toString();
		}

		public String getMessage() {
			return codeString() + " (" + extraString() + ")";
		}

		public String toString() {
			return "<" + getClass().getName() + " code=" + code + "(" + codeString() + ") " + extraString() + ">";
		}
	}

	public interface Returner<T> {
		public void call(T t);
	}

	private final static Object NOLOCK = new Object();

	private Map<Identifier, Collection<Runnable>> waitingOperations = new HashMap<Identifier, Collection<Runnable>>();

	protected Map<Identifier, Object> knownLocks = new ReferenceMap<Identifier, Object>();

	/**
	 * Get a description of this storage.
	 * 
	 * The description consists of a string array where the first element is the class name, and the rest are
	 * constructor arguments.
	 * 
	 * @return a string array where the first element is the class name and the rest are the constructor arguments
	 */
	public String[] getDescription() {
		String[] arguments = getConstructorArguments();
		String[] returnValue = new String[arguments.length + 1];
		System.arraycopy(arguments, 0, returnValue, 1, arguments.length);
		returnValue[0] = getClass().getName();
		return returnValue;
	}

	/**
	 * Get the constructor arguments used to create this instance.
	 * 
	 * @return an array consisting of the constructor arguments for this instance
	 */
	protected abstract String[] getConstructorArguments();

	public static LockingStorage getInstance(String... description) {
		try {
			String klass = description[0];
			String[] args = new String[description.length - 1];
			System.arraycopy(description, 1, args, 0, description.length - 1);
			Class[] constructorArgumentClasses = new Class[args.length];
			for (int i = 0; i < args.length; i++) {
				constructorArgumentClasses[i] = String.class;
			}
			return (LockingStorage) Class.forName(klass).getConstructor(constructorArgumentClasses).newInstance((Object[]) args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void destroy() {
		super.destroy();
		synchronized (knownLocks) {
			knownLocks = new ReferenceMap<Identifier, Object>();
			waitingOperations = new HashMap<Identifier, Collection<Runnable>>();
		}
	}

	/**
	 * Set who is holding a lock on a given identifier.
	 * 
	 * Will increment the iteration of that entry.
	 * 
	 * @param identifier
	 *            the identifier to lock
	 * @param holder
	 *            who is holding the lock
	 * @return true if an entry with that id was updated
	 */
	protected abstract boolean _setLocker(Identifier identifier, Identifier holder);

	private void runWaiting(Identifier identifier) {
		Collection<Runnable> waiting = waitingOperations.remove(identifier);
		if (waiting != null) {
			for (Runnable operation : waiting) {
				operation.run();
			}
		}
	}

	private boolean setLocker(final Identifier identifier, final Identifier holder) {
		boolean returnValue = transaction(new Callable<Boolean>() {
			public Boolean call() {
				boolean returnValue = _setLocker(identifier, holder);
				if (returnValue) {
					merkleInsert(identifier);
					synchronized (knownLocks) {
						knownLocks.put(identifier, lockId(holder));
					}
				}
				return new Boolean(returnValue);
			}
		}).booleanValue();
		if (holder == null) {
			runWaiting(identifier);
		}
		return returnValue;
	}

	private void addWaitingOperation(Identifier identifier, Runnable operation) {
		Collection<Runnable> waiting = waitingOperations.get(identifier);
		if (waiting == null) {
			waiting = new LinkedList<Runnable>();
			waitingOperations.put(identifier, waiting);
		}
		waiting.add(operation);
	}

	public boolean unlock(Identifier identifier, Identifier source) {
		Entry empty = getEmpty(identifier);
		if (empty == null) {
			return false;
		} else if (empty.getLocker() == null) {
			return false;
		} else if (empty.getLocker().equals(source)) {
			return setLocker(identifier, null);
		} else {
			return false;
		}
	}

	public boolean unlock(Entry entry, Identifier source, boolean commutative) {
		Entry empty = getEmpty(entry.getIdentifier());
		if (empty == null) {
			return false;
		} else if (empty.getLocker() == null) {
			return false;
		} else if (empty.getLocker().equals(source)) {
			if (commutative) {
				commutePut(entry);
				updateCache(entry);
			} else {
				execPut(entry);
				updateCache(entry);
			}
			synchronized (knownLocks) {
				knownLocks.put(entry.getIdentifier(), lockId(entry.getLocker()));
			}
			runWaiting(entry.getIdentifier());
			return true;
		} else {
			return false;
		}
	}

	private void lockEmpty(Identifier identifier, Identifier source) {
		put(new Entry(identifier, null, System.currentTimeMillis(), 0, 0, 0, "null", source));
		synchronized (knownLocks) {
			knownLocks.put(identifier, lockId(source));
		}
	}

	public LockResponse tryLock(Identifier identifier, long version, Long commutation, Identifier source) {
		Entry empty = getEmpty(identifier);
		if (empty == null) {
			if (version == 0) {
				lockEmpty(identifier, source);
				return LockResponse.success();
			} else {
				return new LockResponse(0, 0);
			}
		} else if (empty.getVersion() == version && (commutation == null || commutation.longValue() == empty.getCommutation())) {
			if (empty.getLocker() == null) {
				setLocker(identifier, source);
				return LockResponse.success();
			} else if (empty.getLocker().equals(source)) {
				return LockResponse.success();
			} else {
				return LockResponse.failure();
			}
		} else {
			return new LockResponse(empty.getVersion(), empty.getCommutation());
		}
	}

	public void lock(final Identifier identifier, final long version, final Long commutation, final Identifier source,
			final Returner<LockResponse> returner) {
		Entry empty = getEmpty(identifier);
		if (empty == null) {
			if (version == 0) {
				lockEmpty(identifier, source);
				returner.call(LockResponse.success());
			} else {
				returner.call(new LockResponse(0, 0));
			}
		} else if (empty.getVersion() == version && (commutation == null || commutation.longValue() == empty.getCommutation())) {
			if (empty.getLocker() == null) {
				setLocker(identifier, source);
				returner.call(LockResponse.success());
			} else if (empty.getLocker().equals(source)) {
				returner.call(LockResponse.success());
			} else {
				addWaitingOperation(identifier, new Runnable() {
					public void run() {
						LockingStorage.this.lock(identifier, version, commutation, source, returner);
					}
				});
			}
		} else {
			returner.call(new LockResponse(empty.getVersion(), empty.getCommutation()));
		}
	}

	private final Object lockId(Object i) {
		if (i == null) {
			return NOLOCK;
		} else {
			return i;
		}
	}

	private boolean locked(Identifier i, Identifier source) {
		synchronized (knownLocks) {
			if (knownLocks.containsKey(i)) {
				Object knownLocker = knownLocks.get(i);
				return !knownLocker.equals(NOLOCK) && !knownLocker.equals(lockId(source));
			} else {
				ensurePersistExecutor();
				Entry empty = getEmpty(i);
				if (empty == null) {
					knownLocks.put(i, NOLOCK);
					return false;
				} else {
					Identifier knownLocker = empty.getLocker();
					knownLocks.put(i, lockId(knownLocker));
					return knownLocker != null && !knownLocker.equals(source);
				}
			}
		}
	}

	public void del(final Identifier identifier, final Entry oldEntry, final Identifier source, final Returner<Boolean> returner) {
		if (locked(identifier, source)) {
			addWaitingOperation(identifier, new Runnable() {
				public void run() {
					LockingStorage.this.del(identifier, oldEntry, source, returner);
				}
			});
		} else {
			synchronized (knownLocks) {
				knownLocks.put(identifier, NOLOCK);
			}
			returner.call(new Boolean(del(identifier, oldEntry)));
		}
	}

	public void put(final Entry e, final Identifier source, final Returner<Object> returner) {
		if (locked(e.getIdentifier(), source)) {
			addWaitingOperation(e.getIdentifier(), new Runnable() {
				public void run() {
					LockingStorage.this.put(e, source, returner);
				}
			});
		} else {
			put(e);
			synchronized (knownLocks) {
				knownLocks.put(e.getIdentifier(), lockId(e.getLocker()));
			}
			returner.call(null);
		}
	}

	public void update(final Entry oldEntry, final Entry newEntry, final Identifier source, final Returner<Object> returner) {
		update(oldEntry, newEntry);
		returner.call(null);
	}

	/**
	 * Is overridden here because we need to hold CommuteCommands until the identifier it wants to commute is unlocked
	 * (since commutes should not (not even commutes outside transactions) collide with transactions.
	 * 
	 * ExecCommands on the other hand, we have no qualms about letting fight it out with a transactions. Whoever
	 * finishes last will win (last Put will overwrite other Puts to db, and transactions (unlike execs and commutes)
	 * overwrite the cache.
	 */
	public void exec(final DHash dhash, final ExecCommand command) {
		if (command instanceof CommuteCommand) {
			if (locked(command.getIdentifier(), command.getSource())) {
				addWaitingOperation(command.getIdentifier(), new Runnable() {
					public void run() {
						LockingStorage.this.exec(dhash, command);
					}
				});
			} else {
				super.exec(dhash, command);
			}
		} else {
			super.exec(dhash, command);
		}
	}

	public void commutePut(final Entry e, final Identifier source, final Returner<Object> returner) {
		if (locked(e.getIdentifier(), source)) {
			addWaitingOperation(e.getIdentifier(), new Runnable() {
				public void run() {
					LockingStorage.this.commutePut(e, source, returner);
				}
			});
		} else {
			commutePut(e);
			synchronized (knownLocks) {
				knownLocks.put(e.getIdentifier(), lockId(e.getLocker()));
			}
			returner.call(null);
		}
	}

	public void execPut(final Entry e, final Identifier source, final Returner<Object> returner) {
		if (locked(e.getIdentifier(), source)) {
			addWaitingOperation(e.getIdentifier(), new Runnable() {
				public void run() {
					LockingStorage.this.commutePut(e, source, returner);
				}
			});
		} else {
			execPut(e);
			synchronized (knownLocks) {
				knownLocks.put(e.getIdentifier(), lockId(e.getLocker()));
			}
			returner.call(null);
		}
	}

	public void del(final Collection<Entry> c, final Identifier source, final Runnable returner) {
		Collection<Entry> unlocked = new LinkedList<Entry>();
		Iterator<Entry> iterator = c.iterator();
		while (iterator.hasNext()) {
			Entry next = iterator.next();
			if (!locked(next.getIdentifier(), source)) {
				unlocked.add(next);
				iterator.remove();
			}
		}
		del(unlocked);
		synchronized (knownLocks) {
			for (Entry entry : c) {
				knownLocks.put(entry.getIdentifier(), NOLOCK);
			}
		}
		if (c.isEmpty()) {
			returner.run();
		} else {
			addWaitingOperation(c.iterator().next().getIdentifier(), new Runnable() {
				public void run() {
					LockingStorage.this.del(c, source, returner);
				}
			});
		}
	}

	public void put(final Collection<Entry> c, final Identifier source, final Runnable returner) {
		Collection<Entry> unlocked = new LinkedList<Entry>();
		Iterator<Entry> iterator = c.iterator();
		while (iterator.hasNext()) {
			Entry next = iterator.next();
			if (!locked(next.getIdentifier(), source)) {
				unlocked.add(next);
				iterator.remove();
			}
		}
		put(unlocked);
		synchronized (knownLocks) {
			for (Entry entry : c) {
				knownLocks.put(entry.getIdentifier(), lockId(entry.getLocker()));
			}
		}
		if (c.isEmpty()) {
			returner.run();
		} else {
			addWaitingOperation(c.iterator().next().getIdentifier(), new Runnable() {
				public void run() {
					LockingStorage.this.put(c, source, returner);
				}
			});
		}
	}

	private void cleanTransaction(DHash dhash, Identifier identifier) {
		try {
			Object o = getObject(dhash, identifier);
			if (o instanceof TransactionBackend) {
				TransactionBackend t = (TransactionBackend) o;
				if (t.getState() == TransactionBackend.STARTED) {
					if (!t.valid()) {
						debug(this, dhash + " found invalid transaction " + t + ", removing it");
						exec(dhash, new ExecCommand(dhash.getServerInfo(), dhash.getServerInfo(), identifier, "remove"));
					}
				} else if (t.getState() == TransactionBackend.PREPARED) {
					if (!t.busy()) {
						debug(this, dhash + " found crashed transaction " + t + ", aborting it");
						exec(dhash, new ExecCommand(dhash.getServerInfo(), dhash.getServerInfo(), identifier, "abort"));
					}
				} else if (t.getState() == TransactionBackend.COMMITED) {
					if (!t.busy()) {
						warn(this, dhash + " found crashed transaction " + t + ", recommiting it");
						exec(dhash, new ExecCommand(dhash.getServerInfo(), dhash.getServerInfo(), identifier, "recommit", new Long(1000 * 60 * 10)));
					}
				} else if (t.getState() == TransactionBackend.ABORTED) {
					if (!t.busy()) {
						debug(this, dhash + " found crashed transaction " + t + ", reaborting it");
						exec(dhash, new ExecCommand(dhash.getServerInfo(), dhash.getServerInfo(), identifier, "abort"));
					}
				} else {
					warn(this, "Found transaction with id " + t.getIdentifier() + " in illegal state " + t.getState());
				}
			} else {
				warn(this, "Found " + o + " claiming to be a TransactionBackend");
			}
		} catch (Throwable t) {
			error(this, "When trying to clean up " + identifier, t);
		}
	}

	public void cleanTransactions(final DHash dhash) {
		for (Identifier identifier : TransactionBackend.currentBackends()) {
			if (identifier.betweenGT_LTE(dhash.getPredecessor().getIdentifier(), dhash.getIdentifier())) {
				cleanTransaction(dhash, identifier);
			}
		}
		consumeEmpty(TransactionBackend.class.getName(), dhash.getPredecessor().getIdentifier(), dhash.getIdentifier(), new EntryMapConsumer() {
			public String getDescription() {
				return LockingStorage.class.getName() + ".cleanTransactions";
			}

			public int getPriority() {
				return 10;
			}

			public ExecutorService executor() {
				return dhash.getPersistExecutor();
			}

			public int limit() {
				return EntryOfferCommand.MAX_DELIVERY_SIZE;
			}

			public boolean valid(Map<Identifier, Entry> chunk) {
				return true;
			}

			public void consume(final Map<Identifier, Entry> chunk, Runnable restTask) {
				if (chunk.size() > 0) {
					for (Identifier identifier : chunk.keySet()) {
						cleanTransaction(dhash, identifier);
					}
					dhash.getExecutorService().execute(restTask);
				} else {
					dhash.resetTransactionCleanup();
				}
			}
		});
	}

}
