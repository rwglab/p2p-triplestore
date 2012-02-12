/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.commands;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;

import java.net.ConnectException;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.nja.Identifier;

public class SendToSuccessorCommand extends Command {

	private Identifier identifier;

	private Command command;

	public SendToSuccessorCommand(ServerInfo caller, Identifier i, Command c) {
		super(caller);
		identifier = i;
		command = c;
	}

	@Override
	protected Identifier getRegardingIdentifier() {
		return identifier;
	}

	public int getPriority() {
		return -50;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " identifier='" + identifier.toString() + "' command='" + command + "' uuid='" + uuid + "'>";
	}

	@Override
	public void execute(Chord chord, Sender sender) {
		if (chord.getPredecessor() != null && identifier.betweenGT_LTE(chord.getPredecessor().getIdentifier(), chord.getIdentifier())) {
			if (loggable(command, DEBUG))
				debug(command, "" + command + " being executed in final destination by " + this);
			command.run(chord, sender);
		} else if (identifier.betweenGT_LTE(chord.getIdentifier(), chord.getSuccessor().getIdentifier())) {
			try {
				if (loggable(command, DEBUG))
					debug(command, "" + command + " being sent to final destination (" + chord.getSuccessor().getIdentifier().toString() + ") by "
							+ this);
				chord.send(command, chord.getSuccessor().getAddress());
			} catch (ConnectException e) {
				if (loggable(command, DEBUG))
					debug(command, "Error while trying to send " + command + " to " + chord.getSuccessor(), e);
				chord.shiftSuccessors();
				this.run(chord, sender);
			}
		} else {
			ServerInfo closest = chord.closestPrecedingFinger(identifier);
			try {
				chord.send(this, closest.getAddress());
			} catch (ConnectException e) {
				if (loggable(command, DEBUG))
					debug(command, "Error while trying to send " + this + " to " + closest, e);
				chord.clearFingerAddress(closest.getAddress());
				this.run(chord, sender);
			}
		}
	}

}
