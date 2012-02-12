package de.rwglab.p2pts;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import cx.ath.troja.chordless.ChordSet;
import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Dhasher;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ChordBasedSetMapAsync<K, V> implements SetMapAsync<K, V> {

	private final Dhasher dhasher;

	private final Executor executor;

	public ChordBasedSetMapAsync(final Dhasher dhasher, final Executor executor) {
		this.dhasher = dhasher;
		this.executor = executor;
	}

	@Override
	public ListenableFuture<Set<V>> get(final K key, final int timeout, final TimeUnit timeoutTimeUnit) {
		final SettableFuture<Set<V>> future = SettableFuture.create();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					final Delay<ChordSet<V>> delay = dhasher.get(key);
					future.set(delay.get(timeoutTimeUnit.toMillis(timeout)));
				} catch (Delay.TimeoutException e) {
					future.setException(new TimeoutException());
				}
			}
		}
		);
		return future;
	}

	@Override
	public ListenableFuture<Void> put(final K key, final V value, final int timeout, final TimeUnit timeoutTimeUnit) {
		final SettableFuture<Void> future = SettableFuture.create();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ChordSet<V> set = new ChordSet<V>();
					set.add(value);
					Delay<Object> delay = dhasher.put(key, set);
					delay.get(timeoutTimeUnit.toMillis(timeout));
					future.set(null);
				} catch (Delay.TimeoutException e) {
					future.setException(new TimeoutException());
				}
			}
		}
		);
		return future;
	}

	@Override
	public ListenableFuture<Void> remove(final K key, final V value, final int timeout,
										 final TimeUnit timeoutTimeUnit) {

		final SettableFuture<Void> future = SettableFuture.create();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ChordSet<V> set = new ChordSet<V>();
					set.add(value);
					Delay<Boolean> delay = dhasher.del(key, set);
					delay.get(timeoutTimeUnit.toMillis(timeout));
					future.set(null);
				} catch (Delay.TimeoutException e) {
					future.setException(new TimeoutException());
				}
			}
		}
		);
		return future;
	}
}
