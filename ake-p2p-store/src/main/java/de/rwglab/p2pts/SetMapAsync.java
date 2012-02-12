package de.rwglab.p2pts;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface SetMapAsync<K, V> {

	ListenableFuture<Set<V>> get(K key, int timeout, TimeUnit timeoutTimeUnit);

	ListenableFuture<Void> put(K key, V value, int timeout, TimeUnit timeoutTimeUnit);

	ListenableFuture<Void> remove(K key, V value, int timeout, TimeUnit timeoutTimeUnit);

}
