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
import java.util.Arrays;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;

public class StabilizeCommand extends ReturningCommand {

	private ServerInfo predecessor;

	private ServerInfo[] successors;

	public StabilizeCommand(ServerInfo c) {
		super(c);
		predecessor = null;
		successors = null;
	}

	@Override
	public int getPriority() {
		return -100;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " caller='" + caller + "' predecessor='" + predecessor + "' successors='"
				+ (successors == null ? "null" : Arrays.asList(successors)) + "' done='" + done + "' uuid='" + uuid + "'>";
	}

	@Override
	protected void executeHome(Chord chord) {
		if (predecessor == null
				|| (chord.getIdentifier().betweenGT_LT(predecessor.getIdentifier(), chord.getSuccessor().getIdentifier()) && !chord.getIdentifier()
						.equals(predecessor.getIdentifier()))) {
			try {
				chord.send(new SetPredecessorCommand(chord.getServerInfo()), chord.getSuccessor().getAddress());
			} catch (ConnectException e) {
				if (loggable(this, DEBUG))
					debug(this, "Error while trying to send new SetPredecessorCommand(" + caller + ") to " + chord.getSuccessor(), e);
				chord.shiftSuccessors();
			}
		}
		if (predecessor != null && predecessor.getIdentifier().betweenGT_LT(chord.getIdentifier(), chord.getSuccessor().getIdentifier())) {

			chord.setSuccessor(predecessor);
			successors = null;
		}
		if (successors != null) {
			chord.setExtraSuccessors(successors);
		}
	}

	@Override
	protected void executeAway(Chord chord) {
		predecessor = chord.getPredecessor();
		successors = chord.getSuccessorArray();
		chord.ping(this);
		returnHome(chord);
	}

}
