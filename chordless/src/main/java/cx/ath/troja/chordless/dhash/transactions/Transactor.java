/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.transactions;

import java.io.Serializable;
import java.util.Arrays;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.nja.Identifier;

public interface Transactor extends Persister {

	public static class Commutation implements Serializable {
		public Identifier key;

		public String methodName;

		public Object[] arguments;

		public Commutation(Identifier k, String m, Object... a) {
			key = k;
			methodName = m;
			arguments = a;
		}

		public String toString() {
			return ("" + key + "." + methodName + "(" + Arrays.asList(arguments) + ")");
		}
	}

	public Delay<? extends Object> abort(Identifier i, Identifier source);

	public Delay<LockingStorage.LockResponse> prepare(Identifier i, long version, Long commutation, Identifier source);

	public Delay<LockingStorage.LockResponse> tryPrepare(Identifier i, long version, Long commutation, Identifier source);

	public Delay<? extends Object> update(Entry e, Identifier source);

	public Delay<? extends Object> update(Entry e, Identifier source, String methodName, Object... arguments);

	public ClassLoader getClassLoader();

}
