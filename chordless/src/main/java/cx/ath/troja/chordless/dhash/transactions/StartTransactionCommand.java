/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.transactions;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.commands.ReturnValueCommand;
import cx.ath.troja.nja.Identifier;

public class StartTransactionCommand extends ReturnValueCommand<Identifier> {

	protected Identifier identifier;

	public StartTransactionCommand(ServerInfo c, Identifier target) {
		super(c);
		identifier = target;
	}

	@Override
	protected Identifier getRegardingIdentifier() {
		return identifier;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " identifier='" + identifier + "' caller='" + caller + "' returnValue='" + returnValue + "' done='"
				+ done + "' uuid='" + uuid + "'>";
	}

	@Override
	protected void executeHome(DHash dhash) {
		dhash.deliver(this);
	}

	@Override
	protected void executeAway(DHash dhash) {
		new TransactionBackend(identifier);
		returnValue = identifier;
		returnHome(dhash);
	}

}
