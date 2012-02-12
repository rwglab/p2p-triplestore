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

public class SetSuccessorCommand extends ReturningCommand {

	private ServerInfo response;

	public SetSuccessorCommand(ServerInfo c) {
		super(c);
		response = null;
	}

	@Override
	protected Identifier getRegardingIdentifier() {
		if (response == null) {
			return null;
		} else {
			return response.getIdentifier();
		}
	}

	@Override
	public int getPriority() {
		return -100;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " caller='" + caller + "' response='" + response + "' done='" + done + "' uuid='" + uuid + "'>";
	}

	@Override
	protected void executeHome(Chord chord) {
		chord.setSuccessor(response);
	}

	@Override
	protected void executeAway(Chord chord) {
		response = chord.getServerInfo();
		returnHome(chord);
	}

}
