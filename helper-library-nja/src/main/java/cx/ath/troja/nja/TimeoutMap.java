package cx.ath.troja.nja;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TimeoutMap<K, V> {

	private class Entry {
		private Long aTime = System.currentTimeMillis();

		private V value;

		private K key;

		public Entry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}

		public void touch() {
			this.aTime = System.currentTimeMillis();
		}

		public Long getTime() {
			return aTime;
		}
	}

	public interface ValueProducer<V> {
		public V produce();
	}

	public static long DEFAULT_TIMEOUT = 1000 * 60 * 30;

	private ConcurrentMap<K, Entry> entryByKey = new ConcurrentHashMap<K, Entry>();

	private ConcurrentNavigableMap<Long, Map<Entry, Object>> entriesByTime = new ConcurrentSkipListMap<Long, Map<Entry, Object>>();

	private Long timeout = DEFAULT_TIMEOUT;

	private ValueProducer<V> valueProducer = new ValueProducer<V>() {
		public V produce() {
			return null;
		}
	};

	public TimeoutMap() {
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public void setValueProducer(ValueProducer<V> valueProducer) {
		this.valueProducer = valueProducer;
	}

	private void removeFromTimes(Entry entry) {
		Map<Entry, Object> entries = entriesByTime.get(entry.getTime());
		if (entries != null) {
			entries.remove(entry);
		}
	}

	private void addToTimes(Entry entry) {
		entriesByTime.putIfAbsent(entry.getTime(), new ConcurrentHashMap<Entry, Object>());
		entriesByTime.get(entry.getTime()).put(entry, new Object());
	}

	private void clearOldEntries() {
		Iterator<Map.Entry<Long, Map<Entry, Object>>> iterator = entriesByTime.headMap(System.currentTimeMillis() - timeout).entrySet().iterator();
		while (iterator.hasNext()) {
			for (Entry entry : iterator.next().getValue().keySet()) {
				entryByKey.remove(entry.getKey());
			}
			iterator.remove();
		}
	}

	private Entry getEntry(K key) {
		entryByKey.putIfAbsent(key, new Entry(key, valueProducer.produce()));
		Entry returnValue = entryByKey.get(key);

		removeFromTimes(returnValue);
		returnValue.touch();
		addToTimes(returnValue);

		clearOldEntries();

		return returnValue;
	}

	public V get(K key) {
		return getEntry(key).getValue();
	}

	public V putIfAbsent(K key, V value) {
		Entry oldEntry = entryByKey.putIfAbsent(key, new Entry(key, value));
		if (oldEntry == null) {
			return null;
		} else {
			return oldEntry.getValue();
		}
	}

	public void put(K key, V value) {
		getEntry(key).setValue(value);
	}

}