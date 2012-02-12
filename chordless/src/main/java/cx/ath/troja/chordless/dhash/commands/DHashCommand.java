/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.commands.ReturningCommand;
import cx.ath.troja.chordless.dhash.DHash;

public abstract class DHashCommand extends ReturningCommand {

	public DHashCommand(ServerInfo i) {
		super(i);
	}

	protected abstract void executeHome(DHash dhash);

	protected abstract void executeAway(DHash dhash);

	@Override
	protected void executeHome(Chord chord) {
		executeHome((DHash) chord);
	}

	@Override
	protected void executeAway(Chord chord) {
		executeAway((DHash) chord);
	}

	@Override
	protected ExecutorService getExecutor(Chord chord) {
		return ((DHash) chord).getPersistExecutor();
	}

}
