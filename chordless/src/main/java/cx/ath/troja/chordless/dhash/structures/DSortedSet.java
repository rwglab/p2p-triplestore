/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.structures;

import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.nja.Identifier;

import java.util.Map;

public class DSortedSet<V extends Comparable<? super V>> extends DTreap<V, Object> implements DCollection<V> {

	private static final long serialVersionUID = 1L;

	public static class DSortedSetIterator<V extends Comparable<? super V>> implements DCollection.DIterator<V> {
		private DTreap.DTreapIterator<V, Object> backend;

		private Identifier parent;

		public DSortedSetIterator(Identifier p, DTreap.DTreapIterator<V, Object> b) {
			backend = b;
			parent = p;
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
			return backend.next(p).getKey();
		}

		@Override
		public V previous(Persister p) {
			return backend.previous(p).getKey();
		}

		@Override
		public Identifier current(Persister p) {
			return backend.current(p);
		}

		@Override
		public void remove(Persister p) {
			backend.remove(p);
		}

		@Override
		public void add(Persister p, V t) {
			p.exec(parent, "add", t).get();
		}
	}

	private static class InjectWrapper<V, T> extends DMap.DefaultDInject<V, T, Object> {
		private DCollection.DInject<V, T> backend;

		public InjectWrapper(DCollection.DInject<V, T> backend) {
			this.backend = backend;
			this.backend.setRemover(this);
		}

		@Override
		public V inject(V v, Map.Entry<T, Object> e) {
			throw new RuntimeException("Should never be called");
		}

		@Override
		public V perform(V v, DMap.DMapEntry<T, Object> e, Persister p) {
			this.entry = e;
			this.persister = p;
			return this.backend.perform(v, new DMap.DMapEntryWrapper<T>(e), p);
		}
	}

	public DSortedSet() {
		super();
	}

	@Override
	public boolean contains(V v) {
		return super.get(v) != null;
	}

	@Override
	public void add(V v) {
		super.put(v, "");
	}

	@Override
	public void remove(V v) {
		super.delete(v);
	}

	@Override
	public DCollection.DIterator<V> iterator() {
		return new DSortedSetIterator<V>(getIdentifier(), super.entryIterator());
	}

	@Override
	public DCollection.DIterator<V> iterator(Identifier start) {
		return new DSortedSetIterator<V>(getIdentifier(), super.entryIterator(start));
	}

	@Override
	public void each(DCollection.DEach<V> each) {
		inject(null, each);
	}

	@Override
	public <T> T inject(T t, DCollection.DInject<T, V> inject) {
		return super.inject(t, new InjectWrapper<T, V>(inject));
	}

}