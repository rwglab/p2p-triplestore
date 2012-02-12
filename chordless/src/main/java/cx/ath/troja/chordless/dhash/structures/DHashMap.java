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
import java.util.concurrent.atomic.AtomicLong;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Delays;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public class DHashMap<K, V> extends Persistent implements DMap<K, V> {

	private static final long serialVersionUID = 1L;

	public static class DHashMapIterator<K, V> implements DMap.DEntryIterator<K, V> {
		private DCollection.PrimitiveDIterator<Identifier> backend;

		public DHashMapIterator(DCollection.PrimitiveDIterator<Identifier> b) {
			backend = b;
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
		public Map.Entry<K, V> next(Persister p) {
			Identifier identifier = backend.next(p);
			Delay<Map.Entry<K, V>> delay = p.exec(identifier, "getMapEntry");
			return delay.get();
		}

		@Override
		public Map.Entry<K, V> previous(Persister p) {
			Identifier identifier = backend.previous(p);
			Delay<Map.Entry<K, V>> delay = p.exec(identifier, "getMapEntry");
			return delay.get();
		}

		@Override
		public Identifier current(Persister p) {
			return backend.current(p);
		}

		@Override
		public void remove(Persister p) {
			if (backend.getLast() == null) {
				throw new RuntimeException("You can not remove from a DHashMapIterator before you have fetched at least one entry from it");
			} else {
				Delay<Identifier> delay = p.exec(backend.getLast(), "getValue");
				p.exec(delay.get(), "remove").get();
			}
		}
	}

	private static class DCollectionInjector<S, K, V> extends DCollection.DefaultDInject<S, Identifier> {
		private DMap.DInject<S, K, V> inject;

		public DCollectionInjector(DMap.DInject<S, K, V> i) {
			inject = i;
		}

		public S inject(S s, Identifier i) {
			Delay<S> delay = getPersister().exec(i, "inject", inject, s);
			return delay.get();
		}
	}

	private static class ClearInject<K, V> extends DMap.DefaultDInject<Object, K, V> {
		public Object inject(Object o, Map.Entry<K, V> e) {
			remove();
			return null;
		}
	}

	public static class DHashMapEntry<K, V> extends DMap.DMapEntry<K, V> {
		private static final long serialVersionUID = 1L;

		private Identifier element;

		private Identifier map;

		public DHashMapEntry(Identifier m, K k, V v) {
			map = m;
			key = k;
			value = v;
			element = null;
		}

		public void setElement(Identifier i) {
			element = i;
		}

		public Object getId() {
			return DHashMap.identifierFor(map, key);
		}

		public boolean equals(Object o) {
			if (o instanceof DHashMapEntry) {
				return getIdentifier().equals(((DHashMapEntry) o).getIdentifier());
			} else {
				return false;
			}
		}

		public int hashCode() {
			return getIdentifier().hashCode();
		}

		public V setValue(V v) {
			V oldValue = value;
			value = v;
			return oldValue;
		}

		public Object inject(DMap.DInject<Object, K, V> inject, Object sum) {
			return inject.perform(sum, this, this);
		}

		@Override
		public Delay<? extends Object> removeAsync() {
			final Delays delays = new Delays();
			final Transaction t = transaction();
			delays.add(t.exec(element, "remove"));
			delays.add(super.removeAsync());
			delays.add(t.exec(map, "decSize"));
			setTaint(false);
			return new Delay<Object>() {
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
	}

	public static Identifier identifierFor(Identifier map, Object k) {
		return Identifier.generate("" + map + Identifier.generate(k));
	}

	private Identifier id;

	private Identifier collection;

	private AtomicLong size;

	public DHashMap() {
		id = Identifier.random();
		collection = null;
		size = new AtomicLong(0);
	}

	public void setId(Identifier x) {
		id = x;
	}

	public void decSize() {
		size.decrementAndGet();
	}

	public void incSize() {
		size.incrementAndGet();
	}

	public String toString() {
		return "<" + getClass().getName() + ":" + hashCode() + " id=" + id + " size=" + size + ">";
	}

	@Override
	public long size() {
		return size.get();
	}

	public Identifier collection() {
		setTaint(false);
		if (collection == null) {
			setTaint(true);
			DCollection.Primitive<Identifier> dcollection = new DTree<Identifier>();
			put(dcollection).get();
			collection = dcollection.getIdentifier();
		}
		return collection;
	}

	public Object getId() {
		return id;
	}

	public void each(DMap.DEach<K, V> each) {
		inject(null, each);
	}

	public <S> S inject(S s, DMap.DInject<S, K, V> inject) {
		setTaint(false);
		Delay<S> delay = exec(collection(), "inject", s, new DCollectionInjector<S, K, V>(inject));
		return delay.get();
	}

	public void clear() {
		setTaint(false);
		inject(null, new ClearInject<K, V>());
	}

	@Override
	protected Delay<? extends Object> removeAsync() {
		clear();
		setTaint(false);
		final Transaction t = transaction();
		final Delays delays = new Delays();
		delays.add(t.exec(collection(), "remove"));
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

	@Override
	public V get(K k) {
		Delay<DHashMapEntry<K, V>> delay = super.get(identifierFor(getIdentifier(), k));
		setTaint(false);
		if (delay.get() == null) {
			return null;
		} else {
			return delay.get().getValue();
		}
	}

	@Override
	public V del(K k) {
		Delay<DHashMapEntry<K, V>> delay = super.get(identifierFor(getIdentifier(), k));
		DHashMapEntry<K, V> existingEntry = delay.get();
		setTaint(false);
		if (existingEntry == null) {
			return null;
		} else {
			V returnValue = existingEntry.getValue();
			exec(existingEntry.getIdentifier(), "remove").get();
			return returnValue;
		}
	}

	@Override
	public V put(K k, V v) {
		Delay<DHashMapEntry<K, V>> delay = super.get(identifierFor(getIdentifier(), k));
		DHashMapEntry<K, V> existingEntry = delay.get();
		setTaint(false);
		if (existingEntry == null) {
			DHashMapEntry<K, V> entry = new DHashMapEntry<K, V>(getIdentifier(), k, v);
			DCollection.DElement<Identifier> element = new DTree.Node<Identifier>(entry.getIdentifier());
			entry.setElement(element.getIdentifier());
			final Delays delays = new Delays();
			final Transaction t = transaction();
			delays.add(t.put(entry));
			Identifier l = (Identifier) t.exec(getIdentifier(), "collection").get();
			delays.add(t.exec(l, "addElement", element));
			delays.add(t.commute(getIdentifier(), "incSize"));
			delays.get();
			t.commit();
			return null;
		} else {
			V returnValue = existingEntry.getValue();
			super.exec(existingEntry.getIdentifier(), "setValue", v).get();
			return returnValue;
		}
	}

	@Override
	public DHashMapIterator<K, V> entryIterator() {
		Delay<DCollection.PrimitiveDIterator<Identifier>> delay = exec(collection(), "iterator");
		return new DHashMapIterator<K, V>(delay.get());
	}

	@Override
	public DHashMapIterator<K, V> entryIterator(Identifier start) {
		Delay<DCollection.PrimitiveDIterator<Identifier>> delay = exec(collection(), "iterator", start);
		return new DHashMapIterator<K, V>(delay.get());
	}

}