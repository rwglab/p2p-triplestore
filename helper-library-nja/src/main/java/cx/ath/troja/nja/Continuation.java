/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A continuation that works by creating and blocking threads.
 */
public abstract class Continuation implements Runnable {

	private static long counter = 0;

	private String name;

	public Continuation() {
		name = ("Continuation-" + (counter++));
	}

	public String getName() {
		return name;
	}

	public Continuation(String n) {
		name = n;
	}

	private Thread thread = null;

	/**
	 * Does whatever the continuation shall do.
	 */
	public abstract void run();

	protected void resetThread() {
		thread = null;
	}

	public Thread.State getState() {
		if (thread == null) {
			return null;
		} else {
			return thread.getState();
		}
	}

	public String toString() {
		return "<" + this.getClass().getName() + " thread=" + thread + ">";
	}

	/**
	 * Starts this continuation and waits until it either finishes or pauses.
	 * 
	 * Call repeatedly until it returns true to finish the continuation.
	 * 
	 * Will restart the continuation again if called after finished.
	 * 
	 * @return whether the continuation is finished
	 */
	public boolean start() {
		final Collection<RuntimeException> errors = new ArrayList<RuntimeException>();
		synchronized (this) {
			if (thread == null) {
				thread = new Thread(name) {
					public void run() {
						try {
							Continuation.this.run();
						} catch (RuntimeException e) {
							errors.add(e);
						} catch (Throwable t) {
							errors.add(new RuntimeException(t));
						} finally {
							synchronized (Continuation.this) {
								Continuation.this.resetThread();
								Continuation.this.notifyAll();
							}
						}
					}
				};
				thread.start();
			} else if (thread.getState() == Thread.State.WAITING) {
				this.notifyAll();
			}
			if (thread.getState() != Thread.State.TERMINATED) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
		if (errors.size() > 0) {
			throw errors.iterator().next();
		}
		return thread == null;
	}

	/**
	 * Pause this continuation.
	 */
	public void pause(String cause) {
		String oldName = Thread.currentThread().getName();
		Thread.currentThread().setName(oldName + " waiting for Continuation lock");
		synchronized (this) {
			this.notifyAll();
			try {
				Thread.currentThread().setName(oldName + " waiting for " + cause);
				this.wait();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} finally {
				Thread.currentThread().setName(oldName);
			}
		}
	}

}