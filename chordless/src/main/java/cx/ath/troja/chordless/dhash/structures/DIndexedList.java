/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.structures;

import java.util.Map;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Delays;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public class DIndexedList<V> extends Persistent implements DCollection<V> {

	private static final long serialVersionUID = 1L;

	public static class DIndexedListIterator<V> implements DCollection.DIterator<V> {
		private DList.DListIterator<V> backend;

		private Identifier parent;

		private V last;

		public DIndexedListIterator(Identifier p, DList.DListIterator<V> b) {
			backend = b;
			parent = p;
			last = null;
		}

		@Override
		public boolean hasNext(Persister p) {
			return backend.hasNext(p);
		}

		@Override
		public boolean hasPrevious(Persister p) {
			return backend.hasPrevious(p);
		}

		@Override
		public V next(Persister p) {
			last = backend.next(p);
			return last;
		}

		@Override
		public V previous(Persister p) {
			last = backend.previous(p);
			return last;
		}

		@Override
		public Identifier current(Persister p) {
			return backend.current(p);
		}

		@Override
		public void remove(Persister p) {
			p.exec(parent, "remove", last).get();
		}

		@Override
		public void add(Persister p, V t) {
			p.exec(parent, "add", t).get();
		}
	}

	private static class RemoveEachReferred<V> extends DMap.DefaultDEach<V, Identifier> {
		public void each(Map.Entry<V, Identifier> e) {
			getPersister().exec(e.getValue(), "remove").get();
		}
	}

	private static class InjectWrapper<T, V> extends DList.Inject<T, V> {
		private Identifier parent;

		private DCollection.DInject<T, V> backend;

		public InjectWrapper(Identifier parent, DCollection.DInject<T, V> backend) {
			this.parent = parent;
			this.backend = backend;
			this.backend.setRemover(this);
		}

		@Override
		public T inject(T t, V v) {
			throw new RuntimeException("Should never be called");
		}

		@Override
		public T perform(T t, DElement<V> e, Persister p) {
			this.element = e;
			this.persister = p;
			return backend.perform(t, e, p);
		}

		@Override
		public void remove() {
			getPersister().exec(parent, "removeByIdentifier", getElement().getIdentifier(), getElement().getValue()).get();
		}
	}

	private Identifier dList;

	private Identifier dMap;

	private Identifier id;

	public DIndexedList() {
		id = Identifier.random();
		dList = null;
		dMap = null;
	}

	public Object getId() {
		return id;
	}

	private Identifier list() {
		if (dList == null) {
			DList l = new DList();
			dList = l.getIdentifier();
			put(l).get();
		}
		return dList;
	}

	private Identifier map() {
		if (dMap == null) {
			DHashMap m = new DHashMap();
			dMap = m.getIdentifier();
			put(m).get();
		}
		return dMap;
	}

	private Identifier indexListFor(V v) {
		Delay<Identifier> delay = exec(map(), "get", v);
		Identifier returnValue = null;
		if (delay.get() == null) {
			DList l = new DList();
			returnValue = l.getIdentifier();
			Delay<? extends Object> d1 = put(l);
			Delay<? extends Object> d2 = exec(map(), "put", v, returnValue);
			d1.get();
			d2.get();
		} else {
			returnValue = delay.get();
		}
		return returnValue;
	}

	private void indexListPush(V v, Identifier listElement) {
		exec(indexListFor(v), "append", listElement).get();
	}

	private Identifier indexListPop(V v) {
		Identifier indexList = indexListFor(v);
		Delay<Identifier> delay = exec(indexList, "removeFirst");
		delay.get();
		if (exec(indexList, "size").get().equals(new Long(0))) {
			exec(map(), "del", v).get();
		}
		return delay.get();
	}

	@Override
	public long size() {
		Delay<Long> delay = exec(list(), "size");
		return delay.get().longValue();
	}

	public void _add(V v) {
		Delay<Identifier> delay = exec(list(), "append", v);
		indexListPush(v, delay.get());
	}

	@Override
	public void add(V v) {
		Transaction t = transaction();
		t.exec(getIdentifier(), "_add", v).get();
		t.commit();
		setTaint(false);
	}

	@Override
	public DCollection.DIterator<V> iterator() {
		Delay<DList.DListIterator<V>> delay = exec(list(), "iterator");
		return new DIndexedListIterator<V>(getIdentifier(), delay.get());
	}

	@Override
	public boolean contains(V v) {
		return exec(map(), "get", v).get() != null;
	}

	public void _remove(V v) {
		if (contains(v)) {
			exec(indexListPop(v), "remove").get();
		}
	}

	@Override
	public void remove(V v) {
		Transaction t = transaction();
		t.exec(getIdentifier(), "_remove", v).get();
		t.commit();
		setTaint(false);
	}

	@Override
	protected Delay<? extends Object> removeAsync() {
		setTaint(false);
		final Transaction t = transaction();
		final Delays delays = new Delays();
		delays.add(t.exec(list(), "remove"));
		t.exec(map(), "each", new RemoveEachReferred<V>()).get();
		delays.add(t.exec(map(), "remove"));
		delays.add(super.removeAsync());
		return new Delay<Object>() {
			@Override
			public Object get(long timeout) {
				return get();
			}

			public Object get() {
				delays.get();
				t.commit();
				return null;
			}
		};
	}

	public void removeByIdentifier(Identifier i, V v) {
		Transaction t = transaction();
		Delays d = new Delays();
		d.add(t.exec(indexListFor(v), "remove", i));
		d.add(t.exec(i, "remove"));
		d.get();
		t.commit();
	}

	@Override
	public DCollection.DIterator<V> iterator(Identifier start) {
		Delay<DList.DListIterator<V>> delay = exec(list(), "iterator", start);
		return new DIndexedListIterator<V>(getIdentifier(), delay.get());
	}

	@Override
	public void each(DCollection.DEach<V> each) {
		inject(null, each);
	}

	@Override
	public <T> T inject(T t, DInject<T, V> inject) {
		Delay<T> d = exec(dList, "inject", t, new InjectWrapper<T, V>(getIdentifier(), inject));
		return d.get();
	}

}