/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.nja.SerializedObjectHandler;

public class CommandChannel extends SerializedObjectHandler implements Sender {

	private Chord chord;

	public CommandChannel() {
	}

	public CommandChannel(Chord c) {
		this();
		chord = c;
	}

	public void handleClose() {
		if (loggable(this, DEBUG))
			debug(this, "Closing down");
	}

	public void handleObject(Object o) {
		handleCommand((Command) o);
	}

	protected void handleCommand(Command command) {
		output(DEBUG, "" + chord + " received " + command + " from " + remoteAddress);
		command.run(chord, this);
	}

}