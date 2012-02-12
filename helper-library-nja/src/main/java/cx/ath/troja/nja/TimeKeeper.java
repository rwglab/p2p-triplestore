/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * A class that makes it easy to calculate roundtrips and speed of executing things, like selector threads and executor
 * services.
 */
public class TimeKeeper implements Serializable {

	/**
	 * Returns the average time of running the given runnable after having run it a given number of times.
	 * 
	 * @param runnable
	 *            what to run
	 * @param n
	 *            how many times to run it
	 * @return the time it took, in nanoseconds
	 */
	public static long time(Runnable runnable, int n) {
		long before = System.nanoTime();
		for (int i = 0; i < n; i++) {
			runnable.run();
		}
		return (System.nanoTime() - before) / n;
	}

	/**
	 * Returns the average time of running the given runnable after having run once.
	 * 
	 * @param runnable
	 *            what to run
	 * @return the time it took, in nanoseconds
	 */
	public static long time(Runnable runnable) {
		return time(runnable, 1);
	}

	/**
	 * A class that counts the occasions and duration of something.
	 */
	public static class Occurence implements Serializable {
		private long times = 0;

		private long duration = 0;

		private long max = 0;

		private long waitSum = 0;

		private long waitMax = 0;

		private Stack<Long> beginnings = new Stack<Long>();

		/**
		 * {@inheritDoc}
		 */
		public String toString() {
			return "<" + this.getClass().getName() + " times='" + times + "' duration='" + duration + "'>";
		}

		/**
		 * Merge this occurence with another.
		 * 
		 * @param other
		 *            the other Occurence to merge with
		 */
		public void merge(Occurence other) {
			times += other.times;
			duration += other.duration;
			max = Math.max(max, other.max);
			waitSum += other.waitSum;
			waitMax = Math.max(waitMax, other.waitMax);
			synchronized (beginnings) {
				beginnings.addAll(other.beginnings);
			}
		}

		/**
		 * Note the time something begins, and the time it had to wait before beginning.
		 * 
		 * @param wait
		 *            the time it had to wait before beginning
		 */
		public Occurence begin(long wait) {
			waitSum += wait;
			if (wait > waitMax) {
				waitMax = wait;
			}
			synchronized (beginnings) {
				beginnings.push(System.nanoTime());
			}
			return this;
		}

		/**
		 * Note the time something ends, and that it has happened once more.
		 */
		public Occurence end() {
			long now = 0;
			synchronized (beginnings) {
				now = (System.nanoTime() - beginnings.pop());
			}
			if (now > max) {
				max = now;
			}
			duration += now;
			times++;
			return this;
		}

		/**
		 * Get the average wait time for something.
		 * 
		 * @return the average waiting time, in the same unit provided in begin(long wait)
		 */
		public double averageWait() {
			return ((double) waitSum) / ((double) times);
		}

		/**
		 * Get the max wait time for something.
		 * 
		 * @return the max noted waiting time, in the same unit provided in begin(long wait)
		 */
		public double maxWait() {
			return waitMax;
		}

		/**
		 * Get the max duration.
		 * 
		 * @return the max duration (in nanoseconds)
		 */
		public long max() {
			return max;
		}

		/**
		 * Get the average duration.
		 * 
		 * @return duration (in nanoseconds) / times
		 */
		public double averageDuration() {
			return ((double) duration) / ((double) times);
		}

		/**
		 * Get the times it happened.
		 * 
		 * @return the times it happened
		 */
		public long times() {
			return times;
		}

		/**
		 * Get the total duration.
		 * 
		 * @return the duration, in nanoseconds
		 */
		public long duration() {
			return duration;
		}
	}

	private static final int AVG_SIZE = 32;

	private long beforeTimestamp;

	private long afterTimestamp;

	private long[] lastRoundTrips;

	private long[] lastExecutions;

	private int index;

	private Map<String, Occurence> counts = new HashMap<String, Occurence>();

	/**
	 * Create a new time keeper.
	 */
	public TimeKeeper() {
		reset();
	}

	public TimeKeeper clone() {
		TimeKeeper returnValue = new TimeKeeper();
		returnValue.beforeTimestamp = beforeTimestamp;
		returnValue.afterTimestamp = afterTimestamp;
		System.arraycopy(lastRoundTrips, 0, returnValue.lastRoundTrips, 0, lastRoundTrips.length);
		System.arraycopy(lastExecutions, 0, returnValue.lastExecutions, 0, lastExecutions.length);
		returnValue.index = index;
		synchronized (counts) {
			returnValue.counts = new HashMap<String, Occurence>(counts);
		}
		return returnValue;
	}

	/**
	 * Reset this time keeper to a fresh state.
	 */
	public void reset() {
		synchronized (counts) {
			beforeTimestamp = System.nanoTime();
			afterTimestamp = System.nanoTime();
			lastRoundTrips = new long[AVG_SIZE];
			lastExecutions = new long[AVG_SIZE];
			index = 0;
			counts = new HashMap<String, Occurence>();
		}
	}

	/**
	 * Merge the time tables of this with those of other.
	 * 
	 * If entries have the same name, the ones in other will overwrite the ones in this.
	 */
	public void merge(TimeKeeper other) {
		synchronized (counts) {
			for (Map.Entry<String, Occurence> entry : other.getCounts().entrySet()) {
				counts.put(entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * Registers the beginning of something.
	 * 
	 * Will ignore null arguments.
	 * 
	 * @param s
	 *            the name of the something
	 */
	public void begin(String s) {
		begin(s, 0);
	}

	private Occurence getOccurence(String s) {
		synchronized (counts) {
			Occurence o = counts.get(s);
			if (o == null) {
				o = new Occurence();
				counts.put(s, o);
			}
			return o;
		}
	}

	/**
	 * Merge in an Occurence into our statistics for something.
	 * 
	 * @param s
	 *            the name of the something
	 * @param o
	 *            the Occurence to merge in
	 */
	public void merge(String s, Occurence o) {
		getOccurence(s).merge(o);
	}

	/**
	 * Registers the beginning of something, and the time it had to wait before beginning.
	 * 
	 * @param s
	 *            the name of the something
	 * @param wait
	 *            the time it had to wait before beginning
	 */
	public void begin(String s, long wait) {
		if (s != null) {
			Occurence o = getOccurence(s);
			o.begin(wait);
		}
	}

	/**
	 * Registers the end of something.
	 * 
	 * Will ignore null arguments.
	 * 
	 * @param s
	 *            the name of the something
	 */
	public void end(String s) {
		if (s != null) {
			synchronized (counts) {
				Occurence o = counts.get(s);
				if (o != null) {
					o.end();
				}
			}
		}
	}

	/**
	 * Get the things we keep track of.
	 * 
	 * @return the things we keep track of
	 */
	public Map<String, Occurence> getCounts() {
		return counts;
	}

	/**
	 * Sets the before timestamp.
	 */
	public void before() {
		beforeTimestamp = System.nanoTime();
	}

	/**
	 * Calculates the average number of round trips (from one after() call to the next) per second.
	 * 
	 * @return the number of round trips per second
	 */
	public int roundTripsPerSecond() {
		long avg = avgRoundTrip();
		if (avg == 0) {
			return 0;
		} else {
			return (int) ((double) 1000000000.0 / (double) avg);
		}
	}

	/**
	 * Calculate the subjective load of the timed object.
	 * 
	 * The load is calculated as follows: average time spent between before() and after() calls divided by average time
	 * spent between after() and before() calls.
	 * 
	 * @return the subjective load
	 */
	public double load() {
		double avgExec = (double) avgExecution();
		double avgWait = (double) avgRoundTrip() - avgExec;
		if (avgWait == 0d) {
			return Double.MAX_VALUE;
		} else {
			return avgExec / avgWait;
		}
	}

	/**
	 * Calculate the subjective idleness of the timed object.
	 * 
	 * The idleness is the inverse of the load().
	 */
	public double idleness() {
		double load = load();
		if (load == 0d) {
			return Double.MAX_VALUE;
		} else {
			return 1.0d / load;
		}
	}

	/**
	 * Calculates the maximum roundtrips per second the timed object could perform.
	 * 
	 * The number is guessed to be the number of times per second the object could do whatever the object does between
	 * before() and after() calls each second.
	 * 
	 * @return the maximum number of round trips if the object spent no time at all between after() and before() calls
	 */
	public int maxRoundTripsPerSecond() {
		long avg = avgExecution();
		if (avg == 0) {
			return 0;
		} else {
			return (int) ((double) 1000000000.0 / (double) avg);
		}
	}

	/**
	 * Return the average time the timed object takes between making one after() call and the next.
	 * 
	 * @return the time in nanoseconds
	 */
	public long avgRoundTrip() {
		synchronized (counts) {
			long avg = 0;
			for (int i = 0; i < lastRoundTrips.length; i++) {
				avg += lastRoundTrips[i];
			}
			avg += (System.nanoTime() - beforeTimestamp);
			return avg / (lastRoundTrips.length + 1);
		}
	}

	/**
	 * Return the average time the timed object takes between making a before() and after() call.
	 * 
	 * @return the time in nanoseconds
	 */
	public long avgExecution() {
		synchronized (counts) {
			long avg = 0;
			for (int i = 0; i < lastExecutions.length; i++) {
				avg += lastExecutions[i];
			}
			return avg / lastExecutions.length;
		}
	}

	/**
	 * Return the last time after() was called.
	 * 
	 * @return the time in nanoseconds (see System.nanoTime())
	 */
	public long afterTimestamp() {
		return afterTimestamp;
	}

	/**
	 * Return the last time before() was called.
	 * 
	 * @return the time in nanoseconds (see System.nanoTime())
	 */
	public long beforeTimestamp() {
		return beforeTimestamp;
	}

	/**
	 * Sets the after timestamp.
	 */
	public void after() {
		synchronized (counts) {
			long now = System.nanoTime();
			lastExecutions[index] = now - beforeTimestamp;
			lastRoundTrips[index++] = now - afterTimestamp;
			index = index % AVG_SIZE;
			afterTimestamp = now;
		}
	}

}