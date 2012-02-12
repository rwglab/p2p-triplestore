/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.transactions;

import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.warn;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.ExecDhasher;
import cx.ath.troja.chordless.dhash.Persistable;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.storage.ExecStorage;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.chordless.dhash.storage.TaintAware;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Proxist;

public class TransactionBackend extends Persistent {

	private static final long serialVersionUID = 1L;

	public static final int STARTED = 0;

	public static final int PREPARED = 1;

	public static final int COMMITED = 2;

	public static final int ABORTED = 3;

	private static class TransactionDelay<T> implements Delay<T> {
		private T returnValue;

		public TransactionDelay(T t) {
			returnValue = t;
		}

		@Override
		public T get(long timeout) {
			return get();
		}

		@Override
		public T get() {
			if (returnValue instanceof RuntimeException) {
				throw (RuntimeException) returnValue;
			} else {
				return returnValue;
			}
		}
	}

	private class TransactionBackendPersister implements Transactor {
		@Override
		public Delay<Object> put(Object key, Object value) {
			TransactionBackend.this.transactionPut(Identifier.generate(key), value);
			return new TransactionDelay<Object>(null);
		}

		@Override
		public Delay<Object> put(Persistable persistent) {
			return put(persistent.getIdentifier(), persistent);
		}

		@Override
		public Delay<Boolean> del(Object key, Object oldEntry) {
			return new TransactionDelay<Boolean>(new Boolean(TransactionBackend.this.transactionDel(Identifier.generate(key))));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> Delay<T> get(Object key) {
			return new TransactionDelay<T>((T) TransactionBackend.this.transactionGet(Identifier.generate(key)));
		}

		@Override
		public Delay<Long> getVersion(Object key) {
			return new TransactionDelay<Long>(TransactionBackend.this.transactionGetVersion(Identifier.generate(key)));
		}

		@Override
		public Delay<Long> getCommutation(Object key) {
			return new TransactionDelay<Long>(TransactionBackend.this.transactionGetCommutation(Identifier.generate(key)));
		}

		@Override
		public Delay<Boolean> has(Object key) {
			return new TransactionDelay<Boolean>(new Boolean(TransactionBackend.this.transactionHas(Identifier.generate(key))));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> Delay<T> exec(Object key, String methodName, Object... arguments) {
			return new TransactionDelay<T>((T) TransactionBackend.this.transactionExec(Identifier.generate(key), methodName, arguments));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> Delay<T> commute(Object key, String methodName, Object... arguments) {
			return new TransactionDelay<T>((T) TransactionBackend.this.transactionCommute(Identifier.generate(key), methodName, arguments));
		}

		@Override
		public Transaction transaction() {
			return new Transaction(this, TransactionBackend.this.innerTransaction());
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> Delay<T> take(Object key) {
			return new TransactionDelay<T>((T) TransactionBackend.this.transactionTake(Identifier.generate(key)));
		}

		@Override
		public Delay<Boolean> replace(Object key, Object oldValue, Object newValue) {
			return new TransactionDelay<Boolean>(TransactionBackend.this.transactionReplace(Identifier.generate(key), oldValue, newValue));
		}

		@Override
		public Delay<Boolean> replace(Persistable oldEntry, Persistable newEntry) {
			if (oldEntry.getIdentifier().equals(newEntry.getIdentifier())) {
				return replace(oldEntry.getIdentifier(), oldEntry, newEntry);
			} else {
				throw new IllegalArgumentException("Arguments to #replace must have the same identifiers!");
			}
		}

		@Override
		public Delay<? extends Object> abort(Identifier i, Identifier source) {
			return new TransactionDelay<Object>(null);
		}

		@Override
		public Delay<LockingStorage.LockResponse> prepare(Identifier i, long version, Long commutation, Identifier source) {
			return new TransactionDelay<LockingStorage.LockResponse>(TransactionBackend.this.transactionPrepare(i, version, commutation));
		}

		@Override
		public Delay<LockingStorage.LockResponse> tryPrepare(Identifier i, long version, Long commutation, Identifier source) {
			return new TransactionDelay<LockingStorage.LockResponse>(TransactionBackend.this.transactionPrepare(i, version, commutation));
		}

		@Override
		public ClassLoader getClassLoader() {
			return TransactionBackend.this.getClassLoader();
		}

		@Override
		public Delay<? extends Object> update(final Entry e, Identifier source, final String methodName, final Object... arguments) {
			return new Delay<Object>() {
				@Override
				public Object get(long timeout) {
					return get();
				}

				@Override
				public Object get() {
					TransactionBackend.this.transactionCommuteUpdate(e.getIdentifier(), methodName, arguments);
					return null;
				}
			};
		}

		@Override
		public Delay<? extends Object> update(final Entry e, Identifier source) {
			return new Delay<Object>() {
				@Override
				public Object get(long timeout) {
					return get();
				}

				@Override
				public Object get() {
					TransactionBackend.this.transactionVersionUpdate(e.getIdentifier(), e.getValue(getClassLoader()));
					return null;
				}
			};
		}

		@Override
		public <T> Delay<Map.Entry<Identifier, T>> nextEntry(Identifier previous) {
			Map.Entry<Identifier, T> returnValue = TransactionBackend.this.transactionNextEntry(previous);
			return new TransactionDelay<Map.Entry<Identifier, T>>(returnValue);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T, V> Delay<V> envoy(Envoy<T> envoy, Object key) {
			return new TransactionDelay<V>((V) TransactionBackend.this.transactionEnvoy(envoy, Identifier.generate(key)));
		}

	}

	private static Map<Identifier, TransactionBackend> backends = new ConcurrentHashMap<Identifier, TransactionBackend>();

	public static final long MAX_TRANSACTION_AGE = (1000 * 60 * 60 * 24);

	public static TransactionBackend getBackend(Identifier i) {
		return backends.get(i);
	}

	public static void _removeBackend(Identifier i) {
		backends.remove(i);
	}

	public static Collection<Identifier> currentBackends() {
		return backends.keySet();
	}

	private Identifier id;

	private long createdAt;

	private Map<Identifier, Object> content;

	private Map<Identifier, Long> observeVersions;

	private Map<Identifier, Long> modifyVersions;

	private Map<Identifier, Long> observeCommutations;

	private Map<Identifier, Long> modifyCommutations;

	private Map<Identifier, Collection<Transactor.Commutation>> commutations;

	private Map<Identifier, Entry> contentAsEntries;

	private int state;

	private transient Transactor transactor;

	private TransactionBackend() {
		createdAt = System.currentTimeMillis();
		setState(STARTED);
		content = new HashMap<Identifier, Object>();
		observeVersions = new HashMap<Identifier, Long>();
		modifyVersions = new HashMap<Identifier, Long>();
		observeCommutations = new HashMap<Identifier, Long>();
		modifyCommutations = new HashMap<Identifier, Long>();
		commutations = new HashMap<Identifier, Collection<Transactor.Commutation>>();
		contentAsEntries = new HashMap<Identifier, Entry>();
		id = Identifier.random();
	}

	public TransactionBackend(Identifier i) {
		this();
		id = i;
		backends.put(getIdentifier(), this);
	}

	public int memberCount() {
		return content.size();
	}

	public boolean valid() {
		return (System.currentTimeMillis() - createdAt) < MAX_TRANSACTION_AGE;
	}

	private ClassLoader getClassLoader() {
		return getTransactor().getClassLoader();
	}

	private Transactor getTransactor() {
		return (Transactor) getPersister();
	}

	private void writeObject(ObjectOutputStream stream) throws IOException {
		if (state == STARTED) {
			throw new RuntimeException("Serializing STARTED transactions is not allowed");
		}
		if (content.size() > 0) {
			throw new RuntimeException("Serializing transactions with content.size() > 0 is illegal");
		}
		stream.defaultWriteObject();
	}

	private void setState(int s) {
		state = s;
	}

	public Delay<? extends Object> removeAsync() {
		backends.remove(getIdentifier());
		return super.removeAsync();
	}

	public Object getId() {
		return id;
	}

	public int getState() {
		return state;
	}

	public void _setCreatedAt(Long l) {
		createdAt = l;
	}

	public void _breakPrepare(Collection<Identifier> toLock, Long toSleep) {
		try {
			convertContentToEntries();
			prepare(toLock, 1000 * 60 * 60);
			try {
				Thread.sleep(toSleep.longValue());
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		} catch (RuntimeException e) {
			if (!(e.getCause() instanceof InterruptedException))
				warn(this, "error while doing _breakPrepare", e);
		}
	}

	public void _breakCommit(Map<Identifier, Entry> toCommit, Long toSleep) {
		try {
			prepare(1000 * 60 * 60);
			recommit(toCommit, 1000 * 60 * 60);
			try {
				Thread.sleep(toSleep.longValue());
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		} catch (RuntimeException e) {
			if (!(e.getCause() instanceof InterruptedException))
				warn(this, "error while doing _breakCommit", e);
		}
	}

	public void _breakAbort(Long toSleep) {
		executeAbortion();
		try {
			Thread.sleep(toSleep.longValue());
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private void abortMany(Collection<Identifier> toAbort) {
		Collection<Delay<? extends Object>> responses = new LinkedList<Delay<? extends Object>>();
		for (Identifier identifier : toAbort) {
			responses.add(getTransactor().abort(identifier, getIdentifier()));
		}
		for (Delay<? extends Object> delay : responses) {
			delay.get();
		}
	}

	private void executeAbortion() {
		setState(ABORTED);
		save();
		abortMany(contentAsEntries.keySet());
	}

	public boolean abort() {
		if (state == PREPARED || state == ABORTED) {
			executeAbortion();
			remove();
			return true;
		} else if (state == STARTED) {
			remove();
			return true;
		} else if (state == COMMITED) {
			return false;
		} else {
			throw new RuntimeException("Unknown transaction state " + state);
		}
	}

	private void sendRecursivePreparationTries(SortedSet<Identifier> toPrepare, Collection<Delay<LockingStorage.LockResponse>> delays,
			Map<Delay<LockingStorage.LockResponse>, Identifier> identifierByDelay) {
		for (Identifier identifier : toPrepare) {
			Delay<LockingStorage.LockResponse> delay = getTransactor().tryPrepare(identifier, observeVersions.get(identifier),
					modifyVersions.containsKey(identifier) ? observeCommutations.get(identifier) : null, getIdentifier());
			delays.add(delay);
			identifierByDelay.put(delay, identifier);
		}
		toPrepare.clear();
	}

	private void collectRecursivePreparationResults(Map<Delay<LockingStorage.LockResponse>, Identifier> identifierByDelay,
			Collection<Delay<LockingStorage.LockResponse>> delays, SortedSet<Identifier> success, SortedSet<Identifier> toPrepare, long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		for (Delay<LockingStorage.LockResponse> delay : delays) {
			Identifier identifier = identifierByDelay.get(delay);
			LockingStorage.LockResponse result = null;
			try {
				result = delay.get(deadline - System.currentTimeMillis());
			} catch (Delay.TimeoutException e) {
				warn(this, "Timeout while trying to lock " + identifier, e);
				throw new CompromisedTransactionException("Timeout occured while trying to lock " + identifier + " in version "
						+ observeVersions.get(identifier) + " and commutation "
						+ (modifyVersions.containsKey(identifier) ? observeCommutations.get(identifier) : null));
			}
			if (result.code == LockingStorage.LockResponse.LOCK_SUCCESS) {
				success.add(identifier);
			} else if (result.code == LockingStorage.LockResponse.LOCK_ALREADY_LOCKED) {
				toPrepare.add(identifier);
			} else if (result.code == LockingStorage.LockResponse.LOCK_OUTDATED) {
				throw new CompromisedTransactionException(result, identifier, observeVersions.get(identifier),
						modifyVersions.containsKey(identifier) ? observeCommutations.get(identifier) : null);
			} else {
				throw new RuntimeException("Unknown try-prepare result: " + result);
			}
		}
	}

	private void recursiveRePrepare(SortedSet<Identifier> toPrepare, SortedSet<Identifier> alreadyPrepared, long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		Identifier firstFail = toPrepare.first();
		toPrepare.remove(firstFail);
		SortedSet<Identifier> toRePrepare = alreadyPrepared.tailSet(firstFail);
		abortMany(toRePrepare);
		try {
			LockingStorage.LockResponse result = getTransactor().prepare(firstFail, observeVersions.get(firstFail),
					modifyVersions.containsKey(firstFail) ? observeCommutations.get(firstFail) : null, getIdentifier()).get(
					deadline - System.currentTimeMillis());
			if (result.code != LockingStorage.LockResponse.LOCK_SUCCESS) {
				debug(this, "Failed locking " + firstFail + " due to " + result);
				Long newVersion = getVersion(firstFail).get();
				Long newCommutation = getCommutation(firstFail).get();
				throw new CompromisedTransactionException(result, firstFail, observeVersions.get(firstFail),
						modifyVersions.containsKey(firstFail) ? observeCommutations.get(firstFail) : null);
			}
		} catch (Delay.TimeoutException e) {
			warn(this, "Timeout while locking " + firstFail, e);
			throw new CompromisedTransactionException("Timeout occured trying to lock " + firstFail + " in version " + observeVersions.get(firstFail)
					+ " and commutation " + (modifyVersions.containsKey(firstFail) ? observeCommutations.get(firstFail) : null));
		}
		toPrepare.addAll(toRePrepare);
		recursivePreparation(toPrepare, deadline - System.currentTimeMillis());
	}

	private void recursivePreparation(SortedSet<Identifier> toPrepare, long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		Collection<Delay<LockingStorage.LockResponse>> delays = new ArrayList<Delay<LockingStorage.LockResponse>>();
		Map<Delay<LockingStorage.LockResponse>, Identifier> identifierByDelay = new HashMap<Delay<LockingStorage.LockResponse>, Identifier>();

		sendRecursivePreparationTries(toPrepare, delays, identifierByDelay);

		SortedSet<Identifier> success = new TreeSet<Identifier>();

		collectRecursivePreparationResults(identifierByDelay, delays, success, toPrepare, deadline - System.currentTimeMillis());

		if (toPrepare.size() > 0) {
			recursiveRePrepare(toPrepare, success, deadline - System.currentTimeMillis());
		}
	}

	private void recommutate(Collection<Identifier> keys, long timeout) {
		long deadline = System.currentTimeMillis() + timeout;

		Map<Identifier, Entry> localContentAsEntries = contentAsEntries;
		contentAsEntries = new HashMap<Identifier, Entry>();

		Map<Identifier, Collection<Transactor.Commutation>> localCommutations = commutations;
		commutations = new HashMap<Identifier, Collection<Transactor.Commutation>>();

		Map<Identifier, Object> localContent = content;
		content = new HashMap<Identifier, Object>();

		Collection<Entry> commutatedEntries = new ArrayList<Entry>();

		try {
			for (Identifier key : keys) {
				if (modifyCommutations.containsKey(key) && !modifyVersions.containsKey(key)) {
					modifyCommutations.remove(key);
					Collection<Transactor.Commutation> theseCommutations = localCommutations.get(key);
					if (theseCommutations != null) {
						for (Transactor.Commutation commutation : theseCommutations) {
							if (commutation.key.equals(key)) {
								try {
									transactionExecOrCommute(true, commutation.key, commutation.methodName, commutation.arguments);
								} catch (RuntimeException e) {
									throw new CompromisedTransactionException("Failed to run " + commutation, e);
								}
							} else {
								throw new CompromisedTransactionException("A commutation (" + commutation + ") referred a different Identifier than "
										+ key);
							}
						}
					}
					commutatedEntries.add(new Entry(key, content.get(key)));
				}
			}
		} finally {
			contentAsEntries = localContentAsEntries;
			commutations = localCommutations;
			content = localContent;
			for (Entry entry : commutatedEntries) {
				contentAsEntries.put(entry.getIdentifier(), entry);
			}
		}
	}

	private void prepare(Collection<Identifier> keys, long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		setState(PREPARED);
		save();
		SortedSet<Identifier> toPrepare = new TreeSet<Identifier>(keys);

		try {
			recursivePreparation(toPrepare, deadline - System.currentTimeMillis());
			if (getTransactor() instanceof ExecDhasher) {
				recommutate(keys, deadline - System.currentTimeMillis());
			} else if (!(getTransactor() instanceof TransactionBackendPersister)) {
				throw new RuntimeException("" + this + " only handles Transactors that are TransactionBackendPersisters or ExecDhashers");
			}
		} catch (CompromisedTransactionException e) {
			abort();
			throw e;
		}
	}

	private void convertContentToEntries() {
		for (Map.Entry<Identifier, Object> mapEntry : content.entrySet()) {
			contentAsEntries.put(mapEntry.getKey(), new Entry(mapEntry.getKey(), mapEntry.getValue()));
		}
		content.clear();
	}

	private void prepare(long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		convertContentToEntries();
		prepare(contentAsEntries.keySet(), deadline - System.currentTimeMillis());
	}

	private void recommit(Map<Identifier, Entry> entries, long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		setState(COMMITED);
		save();
		Collection<Delay<? extends Object>> responses = new LinkedList<Delay<? extends Object>>();
		for (Map.Entry<Identifier, Entry> mapEntry : entries.entrySet()) {
			if (modifyVersions.containsKey(mapEntry.getKey())) {
				responses.add(getTransactor().update(mapEntry.getValue(), getIdentifier()));
			} else if (modifyCommutations.containsKey(mapEntry.getKey())) {
				if (getTransactor() instanceof TransactionBackendPersister) {
					for (Transactor.Commutation commutation : commutations.get(mapEntry.getKey())) {
						responses.add(getTransactor().update(new Entry(mapEntry.getKey(), null), getIdentifier(), commutation.methodName,
								commutation.arguments));
					}
				} else if (getTransactor() instanceof ExecDhasher) {
					responses.add(getTransactor().update(mapEntry.getValue(), getIdentifier(), null, new Object[0]));
				} else {
					throw new RuntimeException("" + this + " only handles Transactors that are TransactionBackendPersisters or ExecDhashers");
				}
			} else {
				responses.add(getTransactor().abort(mapEntry.getKey(), getIdentifier()));
			}
		}
		for (Delay<? extends Object> delay : responses) {
			delay.get(deadline - System.currentTimeMillis());
		}
	}

	public void recommit(Long timeout) {
		recommit(contentAsEntries, timeout.longValue());
		remove();
	}

	public void commit(Long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		if (state == STARTED) {
			prepare(deadline - System.currentTimeMillis());
			recommit(new Long(deadline - System.currentTimeMillis()));
		} else {
			throw new RuntimeException("Trying to commit a transaction in state " + state + " impossible, only transactions in state " + STARTED
					+ " can be commited");
		}
	}

	private String stateString() {
		if (state == STARTED) {
			return "STARTED";
		} else if (state == PREPARED) {
			return "PREPARED";
		} else if (state == COMMITED) {
			return "COMMITED";
		} else if (state == ABORTED) {
			return "ABORTED";
		} else {
			throw new RuntimeException("Unknown transaction state " + state);
		}
	}

	public String toString() {
		return "<Transaction:" + hashCode() + " id=" + id + " createdAt=" + createdAt + " state=" + stateString() + ">";
	}

	protected LockingStorage.LockResponse transactionPrepare(Identifier i, long version, Long commutation) {
		if ((!modifyVersions.containsKey(i) || modifyVersions.get(i).longValue() == version)
				&& (commutation == null || !modifyCommutations.containsKey(i) || modifyCommutations.get(i).longValue() == commutation.longValue())) {
			return LockingStorage.LockResponse.success();
		} else {
			return new LockingStorage.LockResponse(modifyVersions.get(i) == null ? 0 : modifyVersions.get(i), modifyCommutations.get(i) == null ? 0
					: modifyCommutations.get(i));
		}
	}

	protected void transactionCommuteUpdate(Identifier i, String methodName, Object... arguments) {
		transactionExecOrCommute(true, i, methodName, arguments);
	}

	protected void transactionVersionUpdate(Identifier i, Object o) {
		transactionPut(i, o);
	}

	@Override
	public void setTaint(Boolean tainted) {
	}

	@Override
	public boolean tainted() {
		return false;
	}

	@Override
	public void resetTaint() {
	}

	private void observe(Identifier i) {
		if (!observeVersions.containsKey(i)) {
			observeVersions.put(i, getVersion(i).get());
		}
		if (!observeCommutations.containsKey(i)) {
			observeCommutations.put(i, getCommutation(i).get());
		}
	}

	private void commutationModify(Identifier i) {
		observe(i);
		if (!modifyCommutations.containsKey(i)) {
			modifyCommutations.put(i, observeCommutations.get(i));
		}
		modifyCommutations.put(i, modifyCommutations.get(i) + 1);
	}

	private void commutationModify(Identifier i, String methodName, Object... arguments) {
		commutationModify(i);

		Collection<Transactor.Commutation> theseCommutations = commutations.get(i);
		if (theseCommutations == null) {
			theseCommutations = new ArrayList<Transactor.Commutation>();
			commutations.put(i, theseCommutations);
		}
		theseCommutations.add(new Transactor.Commutation(i, methodName, arguments));
	}

	private void versionModify(Identifier i) {
		observe(i);
		if (!modifyVersions.containsKey(i)) {
			modifyVersions.put(i, observeVersions.get(i));
		}
		modifyVersions.put(i, modifyVersions.get(i) + 1);
	}

	public boolean transactionDel(Identifier i) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		versionModify(i);
		if (content.containsKey(i)) {
			content.put(i, null);
			return true;
		} else {
			content.put(i, null);
			return false;
		}
	}

	public Identifier innerTransaction() {
		TransactionBackend inner = new TransactionBackend();
		content.put(inner.getIdentifier(), inner);
		return inner.getIdentifier();
	}

	public boolean transactionPut(Identifier i, Object o) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		versionModify(i);
		content.put(i, o);
		return true;
	}

	public boolean transactionReplace(Identifier i, Object o, Object n) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		if (content.containsKey(i)) {
			Object old = content.get(i);
			if (Arrays.equals(Cerealizer.pack(old), Cerealizer.pack(o))) {
				versionModify(i);
				content.put(i, o);
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public Object transactionTake(Identifier i) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		if (content.containsKey(i)) {
			Object returnValue = content.get(i);
			versionModify(i);
			content.put(i, null);
			return returnValue;
		} else {
			return null;
		}
	}

	private Object clone(Object o) {
		if (o == null || o instanceof TransactionBackend) {
			return o;
		} else {
			return Cerealizer.unpack(Cerealizer.pack(o), o.getClass().getClassLoader());
		}
	}

	private Object transactionGetHelper(Identifier i) {
		if (content.containsKey(i)) {
			return content.get(i);
		} else {
			observe(i);
			Object returnValue = clone(get(i).get());
			if (returnValue instanceof TaintAware) {
				((TaintAware) returnValue).resetTaint();
			}
			content.put(i, returnValue);
			return returnValue;
		}
	}

	@SuppressWarnings("unchecked")
	public <T> Map.Entry<Identifier, T> transactionNextEntry(Identifier previous) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		Delay<Map.Entry<Identifier, T>> returnValue = nextEntry(previous);
		transactionGetHelper(returnValue.get().getKey());
		return new AbstractMap.SimpleImmutableEntry<Identifier, T>(returnValue.get().getKey(), (T) content.get(returnValue.get().getKey()));
	}

	public Object transactionGet(Identifier i) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		return transactionGetHelper(i);
	}

	public Long transactionGetVersion(Identifier i) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		if (modifyVersions.containsKey(i)) {
			return modifyVersions.get(i);
		} else if (observeVersions.containsKey(i)) {
			return observeVersions.get(i);
		} else {
			transactionGet(i);
			return observeVersions.get(i);
		}
	}

	public Long transactionGetCommutation(Identifier i) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		if (modifyCommutations.containsKey(i)) {
			return modifyCommutations.get(i);
		} else if (observeCommutations.containsKey(i)) {
			return observeCommutations.get(i);
		} else {
			transactionGet(i);
			return observeCommutations.get(i);
		}
	}

	public boolean transactionHas(Identifier i) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		if (content.containsKey(i)) {
			return content.get(i) != null;
		} else {
			observe(i);
			Object o = get(i).get();
			content.put(i, o);
			return o != null;
		}
	}

	private Object transactionExecOrCommute(boolean commute, Identifier key, String methodName, Object... arguments) {
		try {
			Object object = transactionGetHelper(key);
			if (object != null) {
				TaintAware.State taintState = TaintAware.StateProducer.get(object);
				Method method = Proxist.getMethod(object, methodName, arguments);
				Persistent.setPersister(new TransactionBackendPersister(), object);
				Object returnValue = null;
				try {
					returnValue = ExecStorage.exec(object, method, arguments);
				} finally {
					if (taintState.tainted()) {
						if (commute) {
							commutationModify(key, methodName, arguments);
						} else {
							versionModify(key);
						}
					}
					Persistent.clearPersister(object);
				}
				return returnValue;
			} else {
				throw new NoSuchEntryException(key, true);
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Object transactionExec(Identifier key, String methodName, Object... arguments) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		return transactionExecOrCommute(false, key, methodName, arguments);
	}

	public Object transactionCommute(Identifier key, String methodName, Object... arguments) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		return transactionExecOrCommute(true, key, methodName, arguments);
	}

	@SuppressWarnings("unchecked")
	public <T, V> V transactionEnvoy(Envoy<?> envoy, Identifier key) {
		Map<Object, Object> response = new HashMap<Object, Object>();
		transactionEnvoy(response, envoy, key);
		return (V) response.get(null);
	}

	private void transactionEnvoy(final Map<Object, Object> response, final Envoy<?> envoy, Identifier key) {
		if (state != STARTED)
			throw new RuntimeException("This transaction is no longer in state STARTED");
		Object object = transactionGetHelper(key);
		if (object != null) {
			TaintAware.State taintState = TaintAware.StateProducer.get(object);
			envoy.setBackend(new EnvoyBackend() {
				public void returnHome(Object value) {
					response.put(null, value);
				}

				public void redirect(Object key) {
					TransactionBackend.this.transactionEnvoy(response, envoy, Identifier.generate(key));
				}
			});
			Persistent.setPersister(new TransactionBackendPersister(), object);
			try {
				envoy.handleWithCast(object);
			} finally {
				if (taintState.tainted()) {
					versionModify(key);
				}
				Persistent.clearPersister(object);
			}
		} else {
			throw new NoSuchEntryException(key, true);
		}
	}
}
