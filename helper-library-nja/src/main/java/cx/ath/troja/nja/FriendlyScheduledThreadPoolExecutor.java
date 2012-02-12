/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.warn;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An extension to scheduled thread pool executor that both informs about how long the last tasks took in average, and
 * when the last task was finished.
 */
public class FriendlyScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

	private interface InvisibleRunnable extends Runnable {
	}

	private static class PriorityRunnable<V> implements Runnable {
		private int priority;

		private RunnableScheduledFuture<V> task;

		private Runnable runnable;

		private String description;

		private long createdAt;

		public PriorityRunnable(RunnableScheduledFuture<V> task, Runnable runnable, int priority, String description) {
			this.task = task;
			this.runnable = runnable;
			this.priority = priority;
			this.description = description;
			this.createdAt = System.currentTimeMillis();
		}

		public long getCreatedAt() {
			return createdAt;
		}

		public int getPriority() {
			return priority;
		}

		public RunnableScheduledFuture<V> getTask() {
			return task;
		}

		public Runnable getRunnable() {
			return runnable;
		}

		public String getDescription() {
			return description;
		}

		public void run() {
			task.run();
		}

		public String toString() {
			return "<" + this.getClass().getName() + " priority=" + priority + " createdAt=" + createdAt + " description=" + description
					+ " runnable=" + runnable + ">";
		}
	}

	private class InvisibleRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {
		private RunnableScheduledFuture<V> future;

		public InvisibleRunnableScheduledFuture(RunnableScheduledFuture<V> f) {
			future = f;
		}

		public void run() {
			future.run();
		}

		public boolean isPeriodic() {
			return future.isPeriodic();
		}

		public long getDelay(TimeUnit u) {
			return future.getDelay(u);
		}

		public int compareTo(Delayed o) {
			return future.compareTo(o);
		}

		public boolean cancel(boolean b) {
			return future.cancel(b);
		}

		public V get() throws InterruptedException, ExecutionException {
			return future.get();
		}

		public V get(long t, TimeUnit u) throws InterruptedException, ExecutionException, TimeoutException {
			return future.get(t, u);
		}

		public boolean isCancelled() {
			return future.isCancelled();
		}

		public boolean isDone() {
			return future.isDone();
		}
	}

	private class QueueRunningRunnableScheduledFuture<V> extends InvisibleRunnableScheduledFuture<V> {
		public QueueRunningRunnableScheduledFuture(RunnableScheduledFuture<V> f) {
			super(f);
		}

		public void run() {
			FriendlyScheduledThreadPoolExecutor.this.runNextPriority();
		}
	}

	private TimeKeeper timer = new TimeKeeper();

	private PriorityBlockingQueue<PriorityRunnable<? extends Object>> priorityQueue = new PriorityBlockingQueue<PriorityRunnable<? extends Object>>(
			11, new Comparator<PriorityRunnable<? extends Object>>() {
				public int compare(PriorityRunnable<? extends Object> p1, PriorityRunnable<? extends Object> p2) {
					if (p1.getPriority() == p2.getPriority()) {
						return new Long(p1.getCreatedAt()).compareTo(new Long(p2.getCreatedAt()));
					} else {
						return new Integer(p1.getPriority()).compareTo(new Integer(p2.getPriority()));
					}
				}
			});

	private Map<Runnable, String> descriptions = new ConcurrentHashMap<Runnable, String>();

	/**
	 * {@inheritDoc}
	 */
	public FriendlyScheduledThreadPoolExecutor(int corePoolSize) {
		super(corePoolSize);
	}

	/**
	 * {@inheritDoc}
	 */
	public FriendlyScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
		super(corePoolSize, threadFactory);
	}

	protected void runNextPriority() {
		PriorityRunnable<? extends Object> p = null;
		try {
			p = priorityQueue.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		if (timer.avgExecution() > 0 && p.getPriority() > 0 && timer.idleness() < p.getPriority()) {

			final PriorityRunnable<? extends Object> finalP = p;
			schedule(new InvisibleRunnable() {
				public void run() {
					execute(finalP);
				}
			}, p.getPriority() * timer.avgExecution(), TimeUnit.NANOSECONDS);
		} else {
			if (p.getDescription() != null) {
				timer.begin(p.getDescription(), System.currentTimeMillis() - p.getCreatedAt());
			}
			timer.before();
			try {
				p.run();
			} finally {
				timer.after();
				if (p.getDescription() != null) {
					timer.end(p.getDescription());
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <V> RunnableScheduledFuture<V> decorateAny(Object r, RunnableScheduledFuture<V> task) {
		RunnableScheduledFuture<V> returnValue = task;
		if (r instanceof Prioritizable && !task.isPeriodic() && task.getDelay(TimeUnit.NANOSECONDS) <= 0) {

			String description = null;
			if (r instanceof Describable) {
				description = ((Describable) r).getDescription();
			}

			PriorityRunnable<V> p = new PriorityRunnable<V>(task, (Prioritizable) r, ((Prioritizable) r).getPriority(), description);

			priorityQueue.add(p);
			returnValue = new QueueRunningRunnableScheduledFuture<V>(task);
		} else if (r instanceof PriorityRunnable) {
			priorityQueue.add((PriorityRunnable<? extends Object>) r);
			return new QueueRunningRunnableScheduledFuture<V>(((PriorityRunnable<V>) r).getTask());
		} else if (r instanceof InvisibleRunnable) {
			returnValue = new InvisibleRunnableScheduledFuture<V>(task);
		} else if (r instanceof Describable) {
			descriptions.put(task, ((Describable) r).getDescription());
		}

		return returnValue;
	}

	@Override
	protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> c, RunnableScheduledFuture<V> task) {
		return decorateAny(c, task);
	}

	@Override
	protected <V> RunnableScheduledFuture<V> decorateTask(Runnable r, RunnableScheduledFuture<V> task) {
		return decorateAny(r, task);
	}

	/**
	 * Set the timekeeper
	 * 
	 * @param timer
	 *            The TimeKeeper to use
	 */
	public void setTimeKeeper(TimeKeeper timer) {
		this.timer = timer;
	}

	/**
	 * Get the timekeeper for this executor.
	 * 
	 * @return the timekeeper
	 */
	public TimeKeeper getTimeKeeper() {
		return timer;
	}

	/**
	 * Get the priority queue.
	 * 
	 * @return the priority queue
	 */
	public PriorityBlockingQueue<PriorityRunnable<? extends Object>> getPriorityQueue() {
		return priorityQueue;
	}

	/**
	 * Return the total number of tasks in queue.
	 * 
	 * @return the total number of tasks in the priority queue and the scheduled queue.
	 */
	public int getTotalQueueSize() {
		return getPriorityQueue().size() + getQueue().size();
	}

	/**
	 * Get the description map.
	 * 
	 * @return the descriptions
	 */
	public Map<Runnable, String> getDescriptions() {
		return descriptions;
	}

	/**
	 * {@inheritDoc}
	 */
	protected void beforeExecute(Thread t, Runnable r) {
		if (!(r instanceof InvisibleRunnableScheduledFuture)) {
			String description = descriptions.get(r);
			if (description != null) {
				timer.begin(description);
			}
			timer.before();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	protected void afterExecute(Runnable r, Throwable e) {
		if (!(r instanceof InvisibleRunnableScheduledFuture)) {
			timer.after();
			String description = descriptions.remove(r);
			if (description != null) {
				timer.end(description);
			}
		}
		if (e != null) {
			warn(this, "Error executing " + r, e);
		}
	}

}