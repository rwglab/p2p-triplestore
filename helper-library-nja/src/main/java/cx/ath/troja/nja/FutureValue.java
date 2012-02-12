/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2011 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureValue<V> implements Future<V> {

	public static void subscribe(final Runnable subscriber, final FutureValue... values) {
		for (int i = 0; i < values.length; i++) {
			values[i].subscribe(new Runnable() {
				public void run() {
					for (int j = 0; j < values.length; j++) {
						if (!values[j].isDone()) {
							return;
						}
					}
					subscriber.run();
				}
			});
		}
	}

	volatile private boolean cancelled = false;

	volatile private boolean done = false;

	volatile private V value = null;

	volatile private ConcurrentLinkedQueue<Runnable> subscribers = new ConcurrentLinkedQueue<Runnable>();

	@Override
	public String toString() {
		return "<" + super.toString() + " value=" + value + "  done=" + done + "  cancelled=" + cancelled + "  subscribers=" + subscribers + ">";
	}

	@Override
	public boolean cancel(boolean interrupt) {
		cancelled = true;
		return !done;
	}

	@Override
	public V get() throws InterruptedException {
		try {
			return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new InterruptedException("Not really an interrupt - just that we have waited " + Long.MAX_VALUE
					+ " milliseconds, and I am getting impatient.");
		}
	}

	@Override
	public synchronized V get(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
		if (done) {
			return value;
		} else {
			long deadline = System.currentTimeMillis() + timeout;
			this.wait(TimeUnit.MILLISECONDS.convert(timeout, unit));
			if (done) {
				return value;
			} else if (System.currentTimeMillis() < deadline) {
				return get(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
			} else {
				throw new TimeoutException("Waiting for " + this + " for " + timeout + " " + unit);
			}
		}
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	public synchronized void subscribe(Runnable subscriber) {
		if (done) {
			subscriber.run();
		} else {
			subscribers.add(subscriber);
		}
	}

	public synchronized void set(V v) {
		if (done) {
			throw new RuntimeException("You can only set a FutureValue once");
		} else {
			value = v;
			done = true;
			this.notifyAll();
			for (Runnable subscriber : subscribers) {
				subscriber.run();
			}
		}
	}

}
