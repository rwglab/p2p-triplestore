/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.structures;

import cx.ath.troja.chordless.dhash.Persistable;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.nja.Identifier;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;

public interface DMap<K, V> extends Persistable {

	static final long serialVersionUID = 1L;

	public interface DEntryIterator<K, V> extends Serializable {
		public boolean hasPrevious(Persister p);

		public Map.Entry<K, V> previous(Persister p);

		public boolean hasNext(Persister p);

		public Map.Entry<K, V> next(Persister p);

		public Identifier current(Persister p);

		public void remove(Persister p);
	}

	public abstract class DMapEntry<K, V> extends Persistent {
		protected K key;

		protected V value;

		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}

		public Map.Entry<K, V> getMapEntry() {
			return new AbstractMap.SimpleImmutableEntry<K, V>(key, value);
		}

		public void remove() {
			super.remove();
		}
	}

	public static class DMapEntryWrapper<T> extends DCollection.DElement<T> {
		private T value;

		private Identifier identifier;

		private Persister persister;

		public DMapEntryWrapper(DMap.DMapEntry<T, Object> e) {
			this.value = e.getKey();
			this.identifier = e.getIdentifier();
			this.persister = e;
		}

		public Object getId() {
			return identifier;
		}

		public T getValue() {
			return value;
		}

		public void remove() {
			persister.exec(identifier, "remove").get();
		}
	}

	public interface DInject<S, K, V> extends Serializable, DCollection.Remover {
		public S perform(S s, DMapEntry<K, V> e, Persister p);

		public void remove();

		public S inject(S s, Map.Entry<K, V> e);

		public Persister getPersister();

		public DMapEntry<K, V> getEntry();
	}

	public interface DEach<K, V> extends DInject<Object, K, V> {
		public void each(Map.Entry<K, V> e);
	}

	public static abstract class DefaultDInject<S, K, V> implements DInject<S, K, V> {
		protected DMapEntry<K, V> entry;

		protected transient Persister persister;

		@Override
		public Persister getPersister() {
			return persister;
		}

		@Override
		public DMapEntry<K, V> getEntry() {
			return entry;
		}

		@Override
		public S perform(S s, DMapEntry<K, V> e, Persister p) {
			try {
				persister = p;
				entry = e;
				return inject(s, e.getMapEntry());
			} finally {
				entry = null;
			}
		}

		@Override
		public void remove() {
			entry.remove();
		}
	}

	public static abstract class DefaultDEach<K, V> extends DefaultDInject<Object, K, V> implements DEach<K, V> {
		@Override
		public Object inject(Object o, Map.Entry<K, V> e) {
			each(e);
			return null;
		}
	}

	public DEntryIterator<K, V> entryIterator();

	public DEntryIterator<K, V> entryIterator(Identifier start);

	public void each(DEach<K, V> each);

	public <S> S inject(S s, DInject<S, K, V> inject);

	public V create(K k, V v);

	public V delete(K k);

	public V read(K k);

	public long size();

}