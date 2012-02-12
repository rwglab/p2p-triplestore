/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.structures;

import java.io.Serializable;

import cx.ath.troja.chordless.dhash.Persistable;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.nja.Identifier;

public interface DCollection<T> extends Persistable {

	static final long serialVersionUID = 1L;

	public interface Primitive<T> extends DCollection<T> {
		public void addElement(DElement<T> element);
	}

	public interface DIterator<T> extends Serializable {
		public boolean hasNext(Persister p);

		public boolean hasPrevious(Persister p);

		public T next(Persister p);

		public T previous(Persister p);

		public Identifier current(Persister p);

		public void remove(Persister p);

		public void add(Persister p, T t);
	}

	public interface PrimitiveDIterator<T> extends DIterator<T> {
		public Identifier getLast();
	}

	public abstract class DElement<T> extends Persistent {
		protected Identifier id;

		protected T value;

		public Object getId() {
			return id;
		}

		public T getValue() {
			return value;
		}

		public void remove() {
			super.remove();
		}
	}

	public interface Remover {
		public void remove();
	}

	public interface DInject<V, T> extends Serializable, Remover {
		public V perform(V v, DElement<T> e, Persister p);

		public void setRemover(Remover r);

		public void remove();

		public V inject(V v, T t);

		public Persister getPersister();

		public DElement<T> getElement();
	}

	public interface DEach<T> extends DInject<Object, T> {
		public void each(T t);
	}

	public static abstract class DefaultDInject<V, T> implements DInject<V, T> {
		protected Remover remover = this;

		protected DElement<T> element;

		protected transient Persister persister;

		@Override
		public Persister getPersister() {
			return persister;
		}

		@Override
		public DElement<T> getElement() {
			return element;
		}

		@Override
		public V perform(V v, DElement<T> e, Persister p) {
			try {
				persister = p;
				element = e;
				return inject(v, e.getValue());
			} finally {
				element = null;
			}
		}

		public void setRemover(Remover r) {
			remover = r;
		}

		@Override
		public void remove() {
			if (remover == this) {
				element.remove();
			} else {
				remover.remove();
			}
		}
	}

	public static abstract class DefaultDEach<T> extends DefaultDInject<Object, T> implements DEach<T> {
		@Override
		public Object inject(Object o, T t) {
			each(t);
			return null;
		}
	}

	public void each(DEach<T> each);

	public <V> V inject(V v, DInject<V, T> inject);

	public long size();

	public void add(T t);

	public boolean contains(T t);

	public void remove(T t);

	public DIterator<T> iterator();

	public DIterator<T> iterator(Identifier start);

}