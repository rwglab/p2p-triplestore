/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashSet<T> implements Set<T> {

	private ConcurrentHashMap<T, T> backend;

	public ConcurrentHashSet(Collection<T> source) {
		this();
		for (T t : source) {
			backend.put(t, t);
		}
	}

	public ConcurrentHashSet() {
		backend = new ConcurrentHashMap<T, T>();
	}

	@Override
	public Iterator<T> iterator() {
		return backend.keySet().iterator();
	}

	@Override
	public int size() {
		return backend.size();
	}

	@Override
	public boolean add(T t) {
		if (backend.containsKey(t)) {
			return false;
		} else {
			backend.put(t, t);
			return true;
		}
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		boolean returnValue = false;
		for (T t : c) {
			if (add(t)) {
				returnValue = true;
			}
		}
		return returnValue;
	}

	@Override
	public void clear() {
		backend.clear();
	}

	@Override
	public boolean contains(Object o) {
		return backend.containsKey(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (!backend.containsKey(c)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isEmpty() {
		return backend.isEmpty();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean returnValue = false;
		for (Object o : c) {
			if (remove(o)) {
				returnValue = true;
			}
		}
		return returnValue;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean returnValue = false;
		HashSet<?> set = new HashSet<Object>(c);
		Iterator<T> iterator = iterator();
		while (iterator.hasNext()) {
			if (!set.contains(iterator.next())) {
				iterator.remove();
				returnValue = true;
			}
		}
		return returnValue;
	}

	@Override
	public Object[] toArray() {
		return new ArrayList<T>(this).toArray();
	}

	@Override
	public <V> V[] toArray(V[] a) {
		return new ArrayList<T>(this).toArray(a);
	}

	@Override
	public boolean remove(Object o) {
		if (backend.containsKey(o)) {
			backend.remove(o);
			return true;
		} else {
			return false;
		}
	}

}