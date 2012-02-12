/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.commands;

import static cx.ath.troja.nja.Log.output;

import java.util.logging.Level;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;

public class LogCommand extends Command {

	private String message;

	private Level level;

	public LogCommand(ServerInfo c, String m, Level l) {
		super(c);
		message = m;
		level = l;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " message='" + message + "' level='" + level + "' uuid='" + uuid + "'>";
	}

	@Override
	public void execute(Chord chord, Sender sender) {
		output(this, level, message, null);
	}

}
