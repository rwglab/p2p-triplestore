/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class Pool<T> {

	public abstract T create();

	private ConcurrentMap<Thread, T> unavailable = new ConcurrentHashMap<Thread, T>();

	private ConcurrentMap<T, T> available = new ConcurrentHashMap<T, T>();

	public int size() {
		return available() + unavailable();
	}

	public int available() {
		return available.size();
	}

	public int unavailable() {
		return unavailable.size();
	}

	private void checkUnavailable() {
		Iterator<Map.Entry<Thread, T>> iterator = unavailable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Thread, T> entry = iterator.next();
			if (entry.getKey().getState() == Thread.State.TERMINATED) {
				iterator.remove();
				available.put(entry.getValue(), entry.getValue());
			}
		}
	}

	private T getFromAvailable() {
		Iterator<T> iterator = available.keySet().iterator();
		if (iterator.hasNext()) {
			T returnValue = iterator.next();
			iterator.remove();
			return returnValue;
		} else {
			return null;
		}
	}

	public T acquire() {
		T toAcquire = unavailable.get(Thread.currentThread());
		if (toAcquire == null) {
			if (available.size() > 0) {
				T t = getFromAvailable();
				if (t != null) {
					if (unavailable.putIfAbsent(Thread.currentThread(), t) == null) {
						return t;
					} else {
						available.put(t, t);
						return acquire();
					}
				} else {
					return acquire();
				}
			} else {
				checkUnavailable();
				if (available.size() > 0) {
					return acquire();
				} else {
					T t = create();
					if (unavailable.putIfAbsent(Thread.currentThread(), t) == null) {
						return t;
					} else {
						available.put(t, t);
						return acquire();
					}
				}
			}
		} else {
			return toAcquire;
		}
	}

	public void release() {
		T toRelease = unavailable.remove(Thread.currentThread());
		if (toRelease != null) {
			available.put(toRelease, toRelease);
		}
	}

}
