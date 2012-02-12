/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2011 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.warn;

public class DefaultUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

	public void uncaughtException(Thread thread, Throwable throwable) {
		Thread.UncaughtExceptionHandler defaultHandler = Thread.getDefaultUncaughtExceptionHandler();
		if (defaultHandler == null) {
			warn(this, "" + thread + " threw an exception", throwable);
		} else {
			defaultHandler.uncaughtException(thread, throwable);
		}
	}

}