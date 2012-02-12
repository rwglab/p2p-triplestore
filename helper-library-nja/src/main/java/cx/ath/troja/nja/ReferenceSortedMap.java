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
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A sorted map that takes soft, weak or hard references for both values and keys.
 * 
 * Nice for auto-flushing caches.
 */
public class ReferenceSortedMap<K, V> extends ReferenceMap<K, V> implements SortedMap<K, V> {

	protected SortedMap<Referrable<K, V>, Referrable<V, Referrable<K, V>>> backend;

	/**
	 * Creates a map backed by a tree map, using soft references.
	 */
	public ReferenceSortedMap() {
		this(new TreeMap<Referrable<K, V>, Referrable<V, Referrable<K, V>>>(), SoftRef.class, SoftRef.class);
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
	private ReferenceSortedMap(SortedMap<Referrable<K, V>, Referrable<V, Referrable<K, V>>> b, Class k, Class v) {
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
	 * Returns the backing map.
	 * 
	 * @return the backing sorted map
	 */
	@Override
	public SortedMap<Referrable<K, V>, Referrable<V, Referrable<K, V>>> getBackend() {
		return backend;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public K firstKey() {
		checkQueues();
		Referrable<K, V> firstKey = backend.firstKey();
		if (firstKey == null) {
			return null;
		} else {
			return firstKey.get();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public K lastKey() {
		checkQueues();
		Referrable<K, V> lastKey = backend.lastKey();
		if (lastKey == null) {
			return null;
		} else {
			return lastKey.get();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReferenceSortedMap<K, V> headMap(K key) {
		checkQueues();
		Referrable<K, V> realKey = createKeyReference(key);
		return new ReferenceSortedMap<K, V>(backend.headMap(realKey), keyType, valueType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReferenceSortedMap<K, V> tailMap(K key) {
		checkQueues();
		Referrable<K, V> realKey = createKeyReference(key);
		return new ReferenceSortedMap<K, V>(backend.tailMap(realKey), keyType, valueType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReferenceSortedMap<K, V> subMap(K fromKey, K toKey) {
		checkQueues();
		Referrable<K, V> realFromKey = createKeyReference(fromKey);
		Referrable<K, V> realToKey = createKeyReference(toKey);
		return new ReferenceSortedMap<K, V>(backend.subMap(realFromKey, realToKey), keyType, valueType);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Comparator<? super K> comparator() {
		return (Comparator<? super K>) backend.comparator();
	}

}
