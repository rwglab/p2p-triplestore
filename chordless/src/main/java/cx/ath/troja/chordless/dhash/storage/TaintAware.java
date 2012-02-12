/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.storage;

import java.io.Serializable;
import java.util.Arrays;

import cx.ath.troja.nja.Cerealizer;

public interface TaintAware extends Serializable {
	public interface State {
		public boolean tainted();
	}

	public static class DummyState implements State {
		private TaintAware aware;

		public DummyState(TaintAware aware) {
			this.aware = aware;
			aware.resetTaint();
		}

		@Override
		public boolean tainted() {
			return aware.tainted();
		}
	}

	public static class SerializingState implements State {
		private byte[] oldBytes;

		private Object object;

		public SerializingState(Object object) {
			this.object = object;
			oldBytes = Cerealizer.pack(object);
		}

		@Override
		public boolean tainted() {
			return !Arrays.equals(oldBytes, Cerealizer.pack(object));
		}
	}

	public static class StateProducer {
		public static State get(Object o) {
			if (o instanceof TaintAware) {
				return new DummyState((TaintAware) o);
			} else {
				return new SerializingState(o);
			}
		}
	}

	public boolean tainted();

	public void resetTaint();
}