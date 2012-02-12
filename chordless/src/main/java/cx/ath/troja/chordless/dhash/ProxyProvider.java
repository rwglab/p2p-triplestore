/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Proxist;

public interface ProxyProvider extends Persister {

	public static class Forwarder implements Proxist.Forwarder {
		private Identifier identifier;

		private Persister persister;

		public Forwarder(Persister p, Identifier i) {
			identifier = i;
			persister = p;
		}

		public Object forward(String methodName, Object... arguments) {
			Delay<Object> delay = persister.exec(identifier, methodName, arguments);
			return delay.get();
		}

		public Identifier getIdentifier() {
			return identifier;
		}

		public boolean equals(Object other) {
			if (other instanceof Forwarder) {
				return ((Forwarder) other).getIdentifier().equals(getIdentifier());
			} else {
				return false;
			}
		}
	}

	public Object find(Identifier identifier);

}
