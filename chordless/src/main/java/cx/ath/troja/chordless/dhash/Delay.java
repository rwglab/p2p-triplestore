/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

public interface Delay<T> {

	public static class TimeoutException extends RuntimeException {
		public TimeoutException() {
		}

		public TimeoutException(String m) {
			super(m);
		}

		public TimeoutException(Throwable m) {
			super(m);
		}
	}

	public final static long DEFAULT_TIMEOUT = (1000 * 60 * 60 * 24);

	public T get(long timeout);

	public T get();

}