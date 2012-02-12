/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.commands;

import static cx.ath.troja.nja.Log.info;

import java.net.ConnectException;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;

public abstract class ReturningCommand extends Command {

	protected boolean done = false;

	public ReturningCommand(ServerInfo c) {
		super(c);
	}

	protected abstract void executeHome(Chord chord);

	protected abstract void executeAway(Chord chord);

	@Override
	protected void execute(Chord chord, Sender sender) {
		if (done) {
			executeHome(chord);
		} else {
			executeAway(chord);
		}
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " caller='" + caller + "' done='" + done + "' uuid='" + uuid + "'>";
	}

	public void returnHome(Chord chord) {
		done = true;
		try {
			chord.send(this, getCaller().getAddress());
		} catch (ConnectException e) {
			info(this, "Error while trying to send " + this + " home to " + caller, e);
		}
	}

}
