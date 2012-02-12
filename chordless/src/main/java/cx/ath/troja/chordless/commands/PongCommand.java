/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.commands;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.nja.Identifier;

public class PongCommand extends Command {

	private Identifier source;

	private long latency;

	private long sequence;

	public PongCommand(ServerInfo c, long t, Identifier i, long s) {
		super(c);
		latency = System.currentTimeMillis() - t;
		source = i;
		sequence = s;
	}

	public long getSequence() {
		return sequence;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " source='" + source.toString() + "' latency='" + latency + "' sequence='" + sequence + "' uuid='"
				+ uuid + "'>";
	}

	public Identifier getSource() {
		return source;
	}

	public long getLatency() {
		return latency;
	}

	@Override
	public void execute(Chord chord, Sender sender) {
	}

}
