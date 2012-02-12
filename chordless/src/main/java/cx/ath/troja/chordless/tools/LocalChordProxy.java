/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.tools;

import java.util.concurrent.ExecutionException;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.FutureValue;

public class LocalChordProxy implements ChordProxy {

	private class Collector implements Sender {
		private FutureValue<Object> futureReturn = new FutureValue<Object>();

		public void send(Object s) {
			futureReturn.set(Cerealizer.unpack(Cerealizer.pack(s)));
		}

		public Object get() throws InterruptedException, ExecutionException {
			return futureReturn.get();
		}
	}

	private Chord chord;

	public LocalChordProxy(Chord c) {
		chord = c;
	}

	protected Chord getChord() {
		return chord;
	}

	public Object run(final Command c) {
		try {
			final Collector collector = new Collector();
			((Command) Cerealizer.unpack(Cerealizer.pack(c))).run(chord, collector);
			return collector.get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}