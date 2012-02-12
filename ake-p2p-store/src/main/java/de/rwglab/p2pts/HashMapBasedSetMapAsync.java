package de.rwglab.p2pts;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

public class HashMapBasedSetMapAsync<K, V> implements SetMapAsync<K, V> {

	private Map<K, Set<V>> map = newHashMap();

	@Override
	public ListenableFuture<Set<V>> get(final K key, int timeout, TimeUnit timeoutTimeUnit) {
		SettableFuture<Set<V>> future = SettableFuture.create();
		future.set(map.get(key));
		return future;
	}

	@Override
	public ListenableFuture<Void> put(final K key, final V value, int timeout, TimeUnit timeoutTimeUnit) {
		SettableFuture<Void> future = SettableFuture.create();
		Set<V> entry = map.get(key);
		if (entry == null) {
			entry = newHashSet();
			map.put(key, entry);
		}
		entry.add(value);
		future.set(null);
		return future;
	}

	@Override
	public ListenableFuture<Void> remove(final K key, final V value, int timeout, TimeUnit timeoutTimeUnit) {
		SettableFuture<Void> future = SettableFuture.create();
		Set<V> set = map.get(key);
		if (set != null) {
			set.remove(value);
		}
		future.set(null);
		return future;
	}
}
