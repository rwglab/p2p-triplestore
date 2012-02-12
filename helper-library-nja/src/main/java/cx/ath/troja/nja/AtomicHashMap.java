/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2011 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class AtomicHashMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V>, Serializable {

	public static final int MAX_EXPONENT = 32;

	public static final float DEFAULT_LOAD_FACTOR = 0.75f;

	public static final int DEFAULT_INITIAL_CAPACITY = 4;

	public static class AtomicSet<K> extends AbstractSet<K> {
		private class KeyIterator implements Iterator<K> {
			private Iterator<? extends Map.Entry<K, ?>> backend = AtomicSet.this.map.entrySet().iterator();

			@Override
			public boolean hasNext() {
				return backend.hasNext();
			}

			@Override
			public K next() {
				return backend.next().getKey();
			}

			@Override
			public void remove() {
				backend.remove();
			}
		}

		private AtomicHashMap<K, ?> map;

		public AtomicSet() {
			this.map = new AtomicHashMap<K, Object>();
		}

		public AtomicSet(AtomicHashMap<K, ?> map) {
			this.map = map;
		}

		@Override
		public boolean add(K k) {
			if (map.containsKey(k)) {
				return false;
			} else {
				map.put(k, null);
				return true;
			}
		}

		@Override
		public int size() {
			return map.size();
		}

		@Override
		public Iterator<K> iterator() {
			return new KeyIterator();
		}
	}

	public static String bits(long i) {
		StringBuffer rval = new StringBuffer();
		for (int j = 0; j < MAX_EXPONENT; j++) {
			if ((i & (1 << j)) != 0) {
				rval.insert(0, "1");
			} else {
				rval.insert(0, "0");
			}
		}
		return rval.toString();
	}

	public static long unsigned(int i) {
		return (i & 0xffffffffL);
	}

	/*
	 * Ripped from http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel
	 */
	public static int reverse(int v) {
		// swap odd and even bits
		v = ((v >>> 1) & 0x55555555) | ((v & 0x55555555) << 1);
		// swap consecutive pairs
		v = ((v >>> 2) & 0x33333333) | ((v & 0x33333333) << 2);
		// swap nibbles ...
		v = ((v >>> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
		// swap bytes
		v = ((v >>> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
		// swap 2-byte long pairs
		return (v >>> 16) | (v << 16);
	}

	private class EntryIterator implements Iterator<Map.Entry<K, V>> {
		private AtomicLinkedList.AtomicLinkedListIterator<Entry<K, V>> bucket;

		private int bucketIndex;

		public EntryIterator() {
			bucketIndex = 0;
			setBucket();
		}

		private void setBucket() {
			bucket = (AtomicLinkedList.AtomicLinkedListIterator<Entry<K, V>>) getBucketByIndex(bucketIndex).listIterator();
		}

		@Override
		public boolean hasNext() {
			if (bucket.hasNext()) {
				return true;
			} else {
				if (bucketIndex < (1 << exponent.get()) - 1) {
					bucketIndex++;
					setBucket();
					return hasNext();
				} else {
					return false;
				}
			}
		}

		@Override
		public Entry<K, V> next() {
			return bucket.next();
		}

		@Override
		public void remove() {
			bucket.remove();
		}
	}

	private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		@Override
		public int size() {
			return AtomicHashMap.this.size();
		}

		@Override
		public boolean add(Map.Entry<K, V> entry) {
			V v = get(entry.getKey());
			if ((v != null && v.equals(entry.getValue())) || (v == null && entry.getValue() == null)) {
				return false;
			} else {
				put(entry.getKey(), entry.getValue());
				return true;
			}
		}

		@Override
		public boolean contains(Object o) {
			if (o instanceof Map.Entry) {
				Map.Entry entry = (Map.Entry) o;
				V v = get(entry.getKey());
				if ((v != null && v.equals(entry.getValue())) || (v == null && entry.getValue() == null)) {
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			boolean returnValue = false;
			for (Object o : c) {
				returnValue |= remove(o);
			}
			return returnValue;
		}

		@Override
		public boolean remove(Object o) {
			if (o instanceof Map.Entry) {
				Map.Entry entry = (Map.Entry) o;
				return AtomicHashMap.this.remove(entry.getKey(), entry.getValue());
			} else {
				return false;
			}
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator();
		}
	}

	private static class Entry<K, V> implements Map.Entry<K, V> {
		private K key;

		private V value;

		private long hashKey;

		private int hashCode;

		public Entry(K key, V value) {
			this(key == null ? 0 : key.hashCode(), key, value);
		}

		protected Entry(int hashCode, K key, V value) {
			this(hashCode, unsigned(reverse(hashCode)), key, value);
		}

		protected Entry(int hashCode, long hashKey, K key, V value) {
			initialize(hashCode, hashKey, key, value);
		}

		private void initialize(int hashCode, long hashKey, K key, V value) {
			this.hashCode = hashCode;
			this.hashKey = hashKey;
			this.key = key;
			this.value = value;
		}

		@Override
		public String toString() {
			return bits(hashKey) + " [" + hashCode + "] (" + key + " => " + value + ")";
		}

		public long getHashKey() {
			return hashKey;
		}

		public int getHashCode() {
			return hashCode;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof Map.Entry) {
				Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
				return (sameKey(entry) && ((getValue() != null && entry.getValue().equals(getValue())) || (getValue() == null && entry.getValue() == null)));
			} else {
				return false;
			}
		}

		public boolean sameKey(Object o) {
			if (o instanceof Map.Entry) {
				Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
				return (entry != null && ((getKey() != null && getKey().equals(entry.getKey())) || (getKey() == null && entry.getKey() == null)));
			} else {
				return false;
			}
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V newValue) {
			V oldValue = value;
			value = newValue;
			return oldValue;
		}
	}

	private class Search {
		private AtomicLinkedList<Entry<K, V>> bucket;

		private AtomicLinkedList.AtomicLinkedListIterator<Entry<K, V>> bucketIterator;

		private AtomicLinkedList.MatchResult<Entry<K, V>> match;

		private Entry<Object, Object> compare;

		private boolean valueFilter;

		private final AtomicLinkedList.Matcher<Entry<K, V>> keyMatcher = new AtomicLinkedList.Matcher<Entry<K, V>>() {
			public boolean matches(Entry<K, V> e1, Entry<K, V> e2) {
				return e1.sameKey(e2);
			}
		};

		private final AtomicLinkedList.Matcher<Entry<K, V>> valueMatcher = new AtomicLinkedList.Matcher<Entry<K, V>>() {
			public boolean matches(Entry<K, V> e1, Entry<K, V> e2) {
				return e1.equals(e2);
			}
		};

		public Search(Object key) {
			compare = new Entry<Object, Object>(key, null);
			match = null;
			bucket = getBucketByHashCode(compare.getHashCode());
		}

		public boolean matches() {
			return match.matches;
		}

		public Entry<K, V> getMatch() {
			return match.result;
		}

		@SuppressWarnings("unchecked")
		private Search ff() {
			bucketIterator = (AtomicLinkedList.AtomicLinkedListIterator<Entry<K, V>>) bucket.listIterator();
			match = bucketIterator.find(valueFilter ? valueMatcher : keyMatcher, (Entry<K, V>) compare);
			return this;
		}

		@SuppressWarnings("unchecked")
		public void insert(V value, Runnable retry) {
			if (match.matches) {
				throw new RuntimeException("" + getClass() + "#insert(..) is only valid if there was no match");
			} else {
				Entry<K, V> newEntry = new Entry<K, V>(compare.getHashCode(), compare.getHashKey(), (K) compare.getKey(), value);
				if (match.result == null) {
					if (!bucketIterator.append(newEntry, true)) {
						retry.run();
					}
				} else {
					if (!bucketIterator.prepend(newEntry, true)) {
						retry.run();
					}
				}
			}
		}

		public void remove() {
			if (match.matches) {
				bucketIterator.remove();
			} else {
				throw new RuntimeException("" + getClass() + "#remove() is only valid if there was a match");
			}
		}

		public Search value(Object value) {
			valueFilter = true;
			compare.setValue(value);
			return this;
		}
	}

	private final Comparator<Entry<K, V>> ENTRY_COMPARATOR = new Comparator<Entry<K, V>>() {
		public int compare(Entry<K, V> e1, Entry<K, V> e2) {
			return new Long(e1.getHashKey()).compareTo(new Long(e2.getHashKey()));
		}
	};

	/*
	 * 2log(buckets.length)
	 */
	public AtomicInteger exponent = null;

	/*
	 * Array of buckets to use right now
	 */
	public AtomicReferenceArray<AtomicReferenceArray<AtomicLinkedList<Entry<K, V>>>> buckets = null;

	/*
	 * Number of entries in all buckets
	 */
	public AtomicInteger size = null;

	public float loadFactor = DEFAULT_LOAD_FACTOR;

	public AtomicHashMap(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	public AtomicHashMap() {
		this(DEFAULT_INITIAL_CAPACITY);
	}

	public AtomicHashMap(Map<K, V> other) {
		this();
		putAll(other);
	}

	public AtomicHashMap(int initialCapacity, float loadFactor) {
		size = new AtomicInteger(0);
		buckets = new AtomicReferenceArray<AtomicReferenceArray<AtomicLinkedList<Entry<K, V>>>>(MAX_EXPONENT);
		buckets.set(0, new AtomicReferenceArray<AtomicLinkedList<Entry<K, V>>>(1));
		buckets.set(1, new AtomicReferenceArray<AtomicLinkedList<Entry<K, V>>>(1));
		exponent = new AtomicInteger(1);
		this.loadFactor = loadFactor;
		while ((1 << exponent.get()) < Math.max(initialCapacity, DEFAULT_INITIAL_CAPACITY)) {
			grow();
		}
	}

	public void possiblyGrow() {
		if (size.get() > loadFactor * (1 << exponent.get())) {
			grow();
		}
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}

	@Override
	public int size() {
		return size.get();
	}

	@Override
	public boolean containsKey(Object key) {
		Search s = new Search(key).ff();
		if (s.matches()) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public V get(Object key) {
		Search s = new Search(key).ff();
		if (s.matches()) {
			return s.getMatch().getValue();
		} else {
			return null;
		}
	}

	@Override
	public V put(final K key, final V value) {
		Search s = new Search(key).ff();
		if (s.matches()) {
			V oldValue = s.getMatch().getValue();
			s.getMatch().setValue(value);
			return oldValue;
		} else {
			s.insert(value, new Runnable() {
				public void run() {
					put(key, value);
				}
			});
			return null;
		}
	}

	@Override
	public boolean remove(final Object key, final Object value) {
		Search s = new Search(key).value(value).ff();
		if (s.matches()) {
			s.remove();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public V putIfAbsent(final K key, final V value) {
		Search s = new Search(key).ff();
		if (s.matches()) {
			return s.getMatch().getValue();
		} else {
			s.insert(value, new Runnable() {
				public void run() {
					putIfAbsent(key, value);
				}
			});
			return null;
		}
	}

	@Override
	public V replace(K key, V value) {
		Search s = new Search(key).ff();
		if (s.matches()) {
			V oldValue = s.getMatch().getValue();
			s.getMatch().setValue(value);
			return oldValue;
		} else {
			return null;
		}
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		Search s = new Search(key).value(oldValue).ff();
		if (s.matches()) {
			s.getMatch().setValue(newValue);
			return true;
		} else {
			return false;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public V remove(Object key) {
		Search s = new Search(key).ff();
		if (s.matches()) {
			s.remove();
			return s.getMatch().getValue();
		} else {
			return null;
		}
	}

	public void validate() {
		for (int bucketIndex = 0; bucketIndex < (1 << exponent.get()) - 1; bucketIndex++) {
			AtomicLinkedList<Entry<K, V>> bucket = getBucketByIndex(bucketIndex);
			bucket.validate();
		}
	}

	public String toLongString() {
		StringBuffer returnValue = new StringBuffer();
		EntryIterator iterator = (EntryIterator) entrySet().iterator();
		while (iterator.hasNext()) {
			returnValue.append("" + iterator.next() + "\n");
		}
		return returnValue.toString();
	}

	public void grow() {
		int oldExponent = exponent.get();
		int newExponent = oldExponent + 1;
		if (buckets.compareAndSet(newExponent, null, new AtomicReferenceArray<AtomicLinkedList<Entry<K, V>>>(1 << oldExponent))) {
			exponent.set(newExponent);
		}
	}

	public int getBucketIndex(int hashCode) {
		return (int) (hashCode & ((1 << exponent.get()) - 1));
	}

	public AtomicLinkedList<Entry<K, V>> getBucketByHashCode(int hashCode) {
		return getBucketByIndex(getBucketIndex(hashCode));
	}

	public int[] getBucketIndices(int superBucketIndex) {
		int metaBucketIndex = 0;
		int bucketIndex = 0;
		if (superBucketIndex > 0) {
			metaBucketIndex = (int) Math.ceil(Math.log(superBucketIndex + 1) / Math.log(2));
			bucketIndex = superBucketIndex - (1 << (metaBucketIndex - 1));
		}
		return new int[] { metaBucketIndex, bucketIndex };
	}

	public int getNextBucketIndex(long bucketKey) {
		int exp = exponent.get();
		return reverse(((((int) bucketKey) >>> (MAX_EXPONENT - exp)) + 1) << (MAX_EXPONENT - exp));
	}

	public int getPreviousBucketIndex(long bucketKey) {
		int exp = exponent.get();
		return reverse(((((int) bucketKey) >>> (MAX_EXPONENT - exp)) - 1) << (MAX_EXPONENT - exp));
	}

	public AtomicLinkedList<Entry<K, V>> getBucketByIndex(int superBucketIndex) {
		int[] indices = getBucketIndices(superBucketIndex);
		int metaBucketIndex = indices[0];
		int bucketIndex = indices[1];
		AtomicLinkedList<Entry<K, V>> bucket = buckets.get(metaBucketIndex).get(bucketIndex);
		if (bucket == null) {
			bucket = new AtomicLinkedList<Entry<K, V>>(null, ENTRY_COMPARATOR);
			if (superBucketIndex == 0) {
				if (buckets.get(metaBucketIndex).compareAndSet(bucketIndex, null, bucket)) {
					return bucket;
				} else {
					return getBucketByIndex(superBucketIndex);
				}
			} else {
				long bucketKey = unsigned(reverse(superBucketIndex));
				AtomicLinkedList<Entry<K, V>> previousBucket = getBucketByIndex(getPreviousBucketIndex(bucketKey));
				AtomicLinkedList.AtomicLinkedListIterator<Entry<K, V>> iterator = (AtomicLinkedList.AtomicLinkedListIterator<Entry<K, V>>) previousBucket
						.iterator();
				while (iterator.hasNext()) {
					if (iterator.next().getHashKey() >= bucketKey) {
						bucket = new AtomicLinkedList<Entry<K, V>>(iterator, ENTRY_COMPARATOR);
					}
				}
				if (buckets.get(metaBucketIndex).compareAndSet(bucketIndex, null, bucket)) {
					return bucket;
				} else {
					return getBucketByIndex(superBucketIndex);
				}
			}
		} else {
			return bucket;
		}
	}
}
