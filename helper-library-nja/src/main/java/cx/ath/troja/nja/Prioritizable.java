/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

/**
 * Prioritizable instances get executed in the FriendlyScheduledThreadPoolExecutor in priority order, with lowest
 * priority first.
 * 
 * If the priority is > 0 the instance will also not execute until the
 * FriendlyScheduledThreadPoolExecutor#getTimeKeeper()#idleness() is above the priority.
 */
public interface Prioritizable extends Runnable {

	/**
	 * @return the priority of this instance
	 */
	public int getPriority();

}