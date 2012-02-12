/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.transactions;

import java.util.Map;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Persistable;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.ProxyProvider;
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Proxist;

public class Transaction implements ProxyProvider {

	public static class DeadTransactionException extends RuntimeException {
		public DeadTransactionException(String s) {
			super(s);
		}

		public DeadTransactionException(Throwable e) {
			super(e);
		}
	}

	private class TransactionDelay<T> implements Delay<T> {
		private Delay<T> backend;

		public TransactionDelay(Delay<T> b) {
			backend = b;
		}

		public T get(long timeout) {
			T returnValue = null;
			try {
				returnValue = backend.get(timeout);
			} catch (NoSuchEntryException e) {
				if (e.getIdentifier().equals(Transaction.this.transactor)) {
					throw new DeadTransactionException(e);
				} else {
					throw e;
				}
			}
			return returnValue;
		}

		public T get() {
			return get(DEFAULT_TIMEOUT);
		}
	}

	public final static long DEFAULT_TIMEOUT = (1000 * 60);

	private Persister persister;

	private Identifier transactor;

	public Transaction(Persister p, Identifier t) {
		persister = p;
		transactor = t;
	}

	public void abort() {
		try {
			persister.exec(transactor, "abort").get();
		} catch (NoSuchEntryException e) {
		}
	}

	public void commit() {
		commit(DEFAULT_TIMEOUT);
	}

	public void commit(long timeout) {
		try {
			persister.exec(transactor, "commit", new Long(timeout)).get();
		} catch (NoSuchEntryException e) {
			throw new DeadTransactionException(e);
		}
	}

	public Identifier transactionBackend() {
		return transactor;
	}

	public boolean active() {
		try {
			return getState() == TransactionBackend.STARTED;
		} catch (DeadTransactionException e) {
			return false;
		}
	}

	public int getState() {
		try {
			Delay<Integer> delay = persister.exec(transactor, "getState");
			return delay.get().intValue();
		} catch (NoSuchEntryException e) {
			throw new DeadTransactionException(e);
		}
	}

	public String toString() {
		try {
			Delay<String> delay = persister.exec(transactor, "toString");
			return delay.get();
		} catch (NoSuchEntryException e) {
			return "<DEAD TRANSACTION " + transactor + ">";
		}
	}

	@Override
	public Delay<Object> put(Object key, Object value) {
		final Delay<Object> returnValue = persister.exec(transactor, "transactionPut", Identifier.generate(key), value);
		return new TransactionDelay<Object>(returnValue);
	}

	@Override
	public Delay<Object> put(Persistable persistent) {
		return put(persistent.getIdentifier(), persistent);
	}

	@Override
	public Delay<Boolean> del(Object key, Object oldValue) {
		Delay<Boolean> returnValue = persister.exec(transactor, "transactionDel", Identifier.generate(key));
		return new TransactionDelay<Boolean>(returnValue);
	}

	@Override
	public <T> Delay<T> get(Object key) {
		Delay<T> returnValue = persister.exec(transactor, "transactionGet", Identifier.generate(key));
		return new TransactionDelay<T>(returnValue);
	}

	@Override
	public Delay<Long> getVersion(Object key) {
		Delay<Long> returnValue = persister.exec(transactor, "transactionGetVersion", Identifier.generate(key));
		return new TransactionDelay<Long>(returnValue);
	}

	@Override
	public Delay<Long> getCommutation(Object key) {
		Delay<Long> returnValue = persister.exec(transactor, "transactionGetCommutation", Identifier.generate(key));
		return new TransactionDelay<Long>(returnValue);
	}

	@Override
	public Delay<Boolean> has(Object key) {
		Delay<Boolean> returnValue = persister.exec(transactor, "transactionHas", Identifier.generate(key));
		return new TransactionDelay<Boolean>(returnValue);
	}

	@Override
	public <T> Delay<T> exec(Object key, String methodName, Object... arguments) {
		Delay<T> returnValue = persister.exec(transactor, "transactionExec", Identifier.generate(key), methodName, arguments);
		return new TransactionDelay<T>(returnValue);
	}

	@Override
	public <T> Delay<T> commute(Object key, String methodName, Object... arguments) {
		Delay<T> returnValue = persister.exec(transactor, "transactionCommute", Identifier.generate(key), methodName, arguments);
		return new TransactionDelay<T>(returnValue);
	}

	@Override
	public <T> Delay<T> take(Object key) {
		Delay<T> returnValue = persister.exec(transactor, "transactionTake", Identifier.generate(key));
		return new TransactionDelay<T>(returnValue);
	}

	@Override
	public Delay<Boolean> replace(Object key, Object oldValue, Object newValue) {
		Delay<Boolean> returnValue = persister.exec(transactor, "transactionReplace", Identifier.generate(key), oldValue, newValue);
		return new TransactionDelay<Boolean>(returnValue);
	}

	@Override
	public Delay<Boolean> replace(Persistable oldEntry, Persistable newEntry) {
		try {
			if (oldEntry.getIdentifier().equals(newEntry.getIdentifier())) {
				return replace(oldEntry.getIdentifier(), oldEntry, newEntry);
			} else {
				throw new IllegalArgumentException("Arguments to #replace must have the same identifiers!");
			}
		} catch (NoSuchEntryException e) {
			throw new DeadTransactionException(e);
		}
	}

	@Override
	public Transaction transaction() {
		Delay<Identifier> transactorDelay = persister.exec(transactor, "innerTransaction");
		return new Transaction(this, transactorDelay.get());
	}

	@Override
	public <T> Delay<Map.Entry<Identifier, T>> nextEntry(Identifier previous) {
		Delay<Map.Entry<Identifier, T>> returnValue = persister.exec(transactor, "transactionNextEntry", previous);
		return new TransactionDelay<Map.Entry<Identifier, T>>(returnValue);
	}

	@Override
	public <T, V> Delay<V> envoy(Envoy<T> envoy, Object key) {
		Delay<V> returnValue = persister.exec(transactor, "transactionEnvoy", envoy, Identifier.generate(key));
		return new TransactionDelay<V>(returnValue);
	}

	@Override
	public Object find(Identifier identifier) {
		try {
			Delay<Class> classDelay = exec(identifier, "getClass");
			Class<Proxist.Proxy> proxyClass = Proxist.getInstance().proxyFor(classDelay.get());
			Proxist.Proxy proxy = proxyClass.newInstance();
			proxy.setForwarder(new ProxyProvider.Forwarder(this, identifier));
			return proxy;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
