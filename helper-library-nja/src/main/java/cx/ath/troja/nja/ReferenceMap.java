/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReferenceMap<K, V> implements Map<K, V> {

	protected interface Referrable<T, A> {
		public T get();

		public A attachment();
	}

	/**
	 * The soft reference extension used to provide soft reference behaviour to keys and values.
	 * 
	 * You never have to create this manually, just give the class as an argument to the ReferenceSortedMap constructor.
	 */
	public static class SoftRef<T, A> extends SoftReference<T> implements Referrable<T, A>, Comparable {
		private A attachment;

		private int hashCode;

		@SuppressWarnings("unchecked")
		public SoftRef(Object t, Object a, ReferenceQueue q) {
			super((T) t, q);
			hashCode = t.hashCode();
			attachment = (A) a;
		}

		public A attachment() {
			return attachment;
		}

		@SuppressWarnings("unchecked")
		public int compareTo(Object o) {
			if (o instanceof Referrable) {
				Referrable other = (Referrable) o;
				Comparable myGet = (Comparable) get();
				Comparable otherGet = (Comparable) other.get();
				if (myGet == null) {
					if (otherGet == null) {
						return 0;
					} else {
						return -1;
					}
				} else if (otherGet == null) {
					return 1;
				} else {
					return myGet.compareTo(otherGet);
				}
			} else {
				return 0;
			}
		}

		public int hashCode() {
			return hashCode;
		}

		public boolean equals(Object o) {
			if (o instanceof Referrable) {
				Referrable other = (Referrable) o;
				return get() == other.get() || (get() != null && get().equals(other.get()));
			} else {
				return false;
			}
		}
	}

	/**
	 * The weak reference extension used to provide weak reference behaviour to keys and values.
	 * 
	 * You never have to create this manually, just give the class as an argument to the ReferenceSortedMap constructor.
	 */
	public static class WeakRef<T, A> extends WeakReference<T> implements Referrable<T, A>, Comparable {
		private A attachment;

		private int hashCode;

		@SuppressWarnings("unchecked")
		public WeakRef(Object t, Object a, ReferenceQueue q) {
			super((T) t, q);
			hashCode = t.hashCode();
			attachment = (A) a;
		}

		public A attachment() {
			return attachment;
		}

		@SuppressWarnings("unchecked")
		public int compareTo(Object o) {
			if (o instanceof Referrable) {
				Referrable other = (Referrable) o;
				Comparable myGet = (Comparable) get();
				Comparable otherGet = (Comparable) other.get();
				if (myGet == null) {
					if (otherGet == null) {
						return 0;
					} else {
						return -1;
					}
				} else if (otherGet == null) {
					return 1;
				} else {
					return myGet.compareTo(otherGet);
				}
			} else {
				return 0;
			}
		}

		public int hashCode() {
			return hashCode;
		}

		public boolean equals(Object o) {
			if (o instanceof Referrable) {
				Referrable other = (Referrable) o;
				return get() == other.get() || (get() != null && get().equals(other.get()));
			} else {
				return false;
			}
		}
	}

	/**
	 * The hard reference extension used to provide hard reference behaviour to keys and values.
	 * 
	 * You never have to create this manually, just give the class as an argument to the ReferenceSortedMap constructor.
	 */
	public static class HardRef<T, A> implements Referrable<T, A>, Comparable {
		private A attachment;

		private T reference;

		private int hashCode;

		@SuppressWarnings("unchecked")
		public HardRef(Object t, Object a, ReferenceQueue q) {
			reference = (T) t;
			hashCode = t.hashCode();
			attachment = (A) a;
		}

		public T get() {
			return reference;
		}

		public A attachment() {
			return attachment;
		}

		@SuppressWarnings("unchecked")
		public int compareTo(Object o) {
			if (o instanceof Referrable) {
				Referrable other = (Referrable) o;
				Comparable myGet = (Comparable) get();
				Comparable otherGet = (Comparable) other.get();
				if (myGet == null) {
					if (otherGet == null) {
						return 0;
					} else {
						return -1;
					}
				} else if (otherGet == null) {
					return 1;
				} else {
					return myGet.compareTo(otherGet);
				}
			} else {
				return 0;
			}
		}

		public int hashCode() {
			return hashCode;
		}

		public boolean equals(Object o) {
			if (o instanceof Referrable) {
				Referrable other = (Referrable) o;
				return get() == other.get() || get().equals(other.get());
			} else {
				return false;
			}
		}
	}

	protected static class TypedReferenceQueue<T> extends ReferenceQueue<T> {
		@SuppressWarnings("unchecked")
		public T pollTyped() {
			return (T) super.poll();
		}

		@SuppressWarnings("unchecked")
		public T removeTyped() throws InterruptedException {
			return (T) super.remove();
		}

		@SuppressWarnings("unchecked")
		public T removeTyped(long timeout) throws InterruptedException {
			return (T) super.remove(timeout);
		}
	}

	protected Class<Referrable<K, V>> keyType;

	protected Class<Referrable<V, Referrable<K, V>>> valueType;

	protected Map<Referrable<K, V>, Referrable<V, Referrable<K, V>>> backend;

	protected TypedReferenceQueue<Referrable<K, V>> keyQueue = new TypedReferenceQueue<Referrable<K, V>>();

	protected TypedReferenceQueue<Referrable<V, Referrable<K, V>>> valueQueue = new TypedReferenceQueue<Referrable<V, Referrable<K, V>>>();

	protected Constructor<Referrable<K, V>> keyConstructor;

	protected Constructor<Referrable<V, Referrable<K, V>>> valueConstructor;

	/**
	 * Creates a map backed by a hash map, using soft references.
	 */
	public ReferenceMap() {
		this(new HashMap<Referrable<K, V>, Referrable<V, Referrable<K, V>>>(), SoftRef.class, SoftRef.class);
	}

	/**
	 * Creates a map backed by the given sorted map, using the given types of reference.
	 * 
	 * @param b
	 *            the backing sorted map
	 * @param k
	 *            the class of reference for the keys (SoftRef.class, WeakRef.class or HardRef.class)
	 * @param v
	 *            the class of reference for the values (SoftRef.class, WeakRef.class or HardRef.class)
	 */
	@SuppressWarnings("unchecked")
	protected ReferenceMap(Map<Referrable<K, V>, Referrable<V, Referrable<K, V>>> b, Class k, Class v) {
		try {
			backend = b;
			keyType = k;
			valueType = v;
			keyConstructor = keyType.getConstructor(Object.class, Object.class, ReferenceQueue.class);
			valueConstructor = valueType.getConstructor(Object.class, Object.class, ReferenceQueue.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return backend.toString();
	}

	/**
	 * Returns the backing map.
	 * 
	 * @return the backing sorted map
	 */
	public Map<Referrable<K, V>, Referrable<V, Referrable<K, V>>> getBackend() {
		return backend;
	}

	protected void checkQueues() {
		Referrable<K, V> nextKey;
		while ((nextKey = keyQueue.pollTyped()) != null) {
			backend.remove(nextKey);
		}
		Referrable<V, Referrable<K, V>> nextValue;
		while ((nextValue = valueQueue.pollTyped()) != null) {
			backend.remove(nextValue.attachment());
		}
	}

	protected Referrable<K, V> createKeyReference(K key) {
		try {
			return keyConstructor.newInstance(key, null, keyQueue);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected Referrable<V, Referrable<K, V>> createValueReference(Referrable<K, V> keyReference, V value) {
		try {
			return valueConstructor.newInstance(value, keyReference, valueQueue);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V put(K key, V value) {
		checkQueues();
		Referrable<K, V> realKey = createKeyReference(key);
		Referrable<V, Referrable<K, V>> current = backend.put(realKey, createValueReference(realKey, value));
		if (current == null) {
			return null;
		} else {
			return current.get();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		checkQueues();
		Referrable<K, V> realKey = createKeyReference((K) key);
		Referrable<V, Referrable<K, V>> value = backend.get(realKey);
		if (value == null) {
			return null;
		} else {
			return value.get();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		checkQueues();
		Referrable<K, V> realKey = createKeyReference((K) key);
		Referrable<V, Referrable<K, V>> value = backend.remove(realKey);
		if (value == null) {
			return null;
		} else {
			return value.get();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		checkQueues();
		Referrable<K, V> realKey = createKeyReference((K) key);
		return backend.containsKey(realKey);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean containsValue(Object value) {
		checkQueues();
		Referrable<V, Referrable<K, V>> realValue = createValueReference(null, (V) value);
		return backend.containsValue(realValue);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {
		checkQueues();
		return backend.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		checkQueues();
		return backend.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		checkQueues();
		backend.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<K> keySet() {
		checkQueues();
		Set<K> returnValue = new HashSet<K>();
		for (Referrable<K, V> referenceKey : backend.keySet()) {
			K key;
			if ((key = referenceKey.get()) != null) {
				returnValue.add(key);
			}
		}
		return returnValue;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		checkQueues();
		Set<Map.Entry<K, V>> returnValue = new HashSet<Map.Entry<K, V>>();
		for (Map.Entry<Referrable<K, V>, Referrable<V, Referrable<K, V>>> entry : backend.entrySet()) {
			K key = entry.getKey().get();
			V value = entry.getValue().get();
			if (key != null && value != null) {
				returnValue.add(new AbstractMap.SimpleEntry<K, V>(key, value));
			}
		}
		return returnValue;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<V> values() {
		checkQueues();
		Collection<V> returnValue = new ArrayList<V>(backend.size());
		for (Referrable<V, Referrable<K, V>> valueReference : backend.values()) {
			V value;
			if ((value = valueReference.get()) != null) {
				returnValue.add(value);
			}
		}
		return returnValue;
	}

}