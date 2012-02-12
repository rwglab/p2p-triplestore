/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import java.io.ObjectStreamException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import cx.ath.troja.chordless.dhash.storage.TaintAware;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Identifier;

public abstract class Persistent implements TaintAware, Persister, Persistable {

	private class TaintState {
		private byte[] oldBytes;

		private Boolean forcedTaint;

		public TaintState() {
			oldBytes = Cerealizer.pack(Persistent.this);
			forcedTaint = null;
		}

		public void setTaint(Boolean b) {
			forcedTaint = b;
		}

		public boolean tainted() {
			if (forcedTaint == null) {
				return !Arrays.equals(oldBytes, Cerealizer.pack(Persistent.this));
			} else {
				return forcedTaint;
			}
		}
	}

	private class TaintStack {
		private ThreadLocal<Stack<TaintState>> stack = new ThreadLocal<Stack<TaintState>>() {
			protected Stack<TaintState> initialValue() {
				return new Stack<TaintState>();
			}
		};

		public void resetTaint() {
			stack.get().push(new TaintState());
		}

		public boolean tainted() {
			return stack.get().pop().tainted();
		}

		public void setTaint(boolean b) {
			stack.get().peek().setTaint(b);
		}
	}

	private class PersisterCounter {
		public int references;

		public Persister persister;

		public PersisterCounter(Persister persister) {
			this.references = 1;
			this.persister = persister;
		}

		public void increment() {
			references++;
		}

		public void decrement() {
			references--;
		}
	}

	private static final long serialVersionUID = 1L;

	private static Map<Object, Thread> threadByObject = Collections.synchronizedMap(new HashMap<Object, Thread>());

	public static void setPersister(Persister p, Object o) {
		threadByObject.put(o, Thread.currentThread());
		if (o instanceof Persistent) {
			((Persistent) o).setPersister(p);
		}
	}

	public static void clearPersister(Object o) {
		threadByObject.remove(o);
		if (o instanceof Persistent) {
			((Persistent) o).clearPersister();
		}
	}

	public static boolean busy(Object o) {
		return threadByObject.containsKey(o);
	}

	private transient Map<Thread, PersisterCounter> persisterByThread = Collections.synchronizedMap(new HashMap<Thread, PersisterCounter>());

	private transient TaintStack taintStack = new TaintStack();

	protected Object readResolve() throws ObjectStreamException {
		persisterByThread = Collections.synchronizedMap(new HashMap<Thread, PersisterCounter>());
		taintStack = new TaintStack();
		return this;
	}

	private void setPersister(Persister p) {
		if (persisterByThread.containsKey(Thread.currentThread())) {
			persisterByThread.get(Thread.currentThread()).increment();
		} else {
			persisterByThread.put(Thread.currentThread(), new PersisterCounter(p));
		}
	}

	private void clearPersister() {
		PersisterCounter counter = persisterByThread.get(Thread.currentThread());
		if (counter == null) {
			throw new RuntimeException("No persister set for " + this + " in " + Thread.currentThread());
		} else {
			counter.decrement();
			if (counter.references < 1) {
				persisterByThread.remove(Thread.currentThread());
			}
		}
	}

	protected Persister getPersister() {
		PersisterCounter counter = persisterByThread.get(Thread.currentThread());
		if (counter == null) {
			throw new RuntimeException("" + this + " doesn't have a Persister in " + Thread.currentThread());
		}
		return counter.persister;
	}

	public boolean busy() {
		return busy(this);
	}

	@Override
	public boolean tainted() {
		return taintStack.tainted();
	}

	public void setTaint(Boolean tainted) {
		taintStack.setTaint(tainted);
	}

	@Override
	public void resetTaint() {
		taintStack.resetTaint();
	}

	public void save() {
		saveAsync().get();
	}

	public Delay<Object> saveAsync() {
		final Delay<Object> returnValue = put(this);
		resetTaint();
		return returnValue;
	}

	public void remove() {
		removeAsync().get();
	}

	protected Delay<? extends Object> removeAsync() {
		setTaint(false);
		return del(getIdentifier(), null);
	}

	public abstract Object getId();

	public Identifier getIdentifier() {
		return Identifier.generate(getId());
	}

	public Delay<Object> put(Object key, Object value) {
		return getPersister().put(key, value);
	}

	public Delay<Object> put(Persistable persistent) {
		return getPersister().put(persistent);
	}

	public Delay<Boolean> del(Object key, Object oldEntry) {
		return getPersister().del(key, oldEntry);
	}

	public <T> Delay<T> get(Object key) {
		return getPersister().get(key);
	}

	public Delay<Long> getVersion(Object key) {
		return getPersister().getVersion(key);
	}

	public Delay<Long> getCommutation(Object key) {
		return getPersister().getCommutation(key);
	}

	public Delay<Boolean> has(Object key) {
		return getPersister().has(key);
	}

	public <T> Delay<T> exec(Object key, String methodName, Object... arguments) {
		return getPersister().exec(key, methodName, arguments);
	}

	public <T> Delay<T> commute(Object key, String methodName, Object... arguments) {
		return getPersister().commute(key, methodName, arguments);
	}

	public <T, V> Delay<V> envoy(Envoy<T> envoy, Object key) {
		return getPersister().envoy(envoy, key);
	}

	public Transaction transaction() {
		return getPersister().transaction();
	}

	public <T> Delay<T> take(Object key) {
		return getPersister().take(key);
	}

	public <T> Delay<Map.Entry<Identifier, T>> nextEntry(Identifier previous) {
		return getPersister().nextEntry(previous);
	}

	public Delay<Boolean> replace(Object key, Object oldValue, Object newValue) {
		return getPersister().replace(key, oldValue, newValue);
	}

	public Delay<Boolean> replace(Persistable oldEntry, Persistable newEntry) {
		return getPersister().replace(oldEntry, newEntry);
	}

}
