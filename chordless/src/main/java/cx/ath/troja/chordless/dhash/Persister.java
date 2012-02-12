/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import java.io.Serializable;
import java.util.Map;

import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public interface Persister {

	public interface EnvoyBackend {
		public void returnHome(Object value);

		public void redirect(Object key);
	}

	public abstract class Envoy<T> implements Serializable {
		private transient EnvoyBackend backend;

		public void setBackend(EnvoyBackend b) {
			backend = b;
		}

		public void returnHome(Object value) {
			backend.returnHome(value);
		}

		public void redirect(Object key) {
			backend.redirect(key);
		}

		@SuppressWarnings("unchecked")
		public void handleWithCast(Object o) {
			handle((T) o);
		}

		public abstract void handle(T object);
	}

	public Delay<Object> put(Object key, Object value);

	public Delay<Object> put(Persistable persistent);

	public Delay<Boolean> del(Object key, Object oldValue);

	public <T> Delay<T> take(Object key);

	public Delay<Boolean> replace(Object key, Object oldValue, Object newValue);

	public Delay<Boolean> replace(Persistable oldEntry, Persistable newEntry);

	public <T> Delay<T> get(Object key);

	public Delay<Long> getVersion(Object key);

	public Delay<Long> getCommutation(Object key);

	public Delay<Boolean> has(Object key);

	public <T> Delay<T> exec(Object key, String methodName, Object... arguments);

	public <T> Delay<T> commute(Object key, String methodName, Object... arguments);

	public <T, V> Delay<V> envoy(Envoy<T> envoy, Object key);

	public <T> Delay<Map.Entry<Identifier, T>> nextEntry(Identifier previous);

	public Transaction transaction();

}
