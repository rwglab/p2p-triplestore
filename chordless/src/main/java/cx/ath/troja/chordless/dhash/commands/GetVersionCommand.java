/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.nja.Identifier;

public class GetVersionCommand extends ForwardableCommand<Long> {

	private Identifier identifier;

	public GetVersionCommand(ServerInfo c, Identifier i) {
		super(c);
		identifier = i;
	}

	protected Long getResponse(Entry e) {
		return e.getVersion();
	}

	@Override
	protected void executeHome(DHash dhash) {
		dhash.deliver(this);
	}

	@Override
	protected void executeAway(DHash dhash) {
		Entry empty = dhash.getStorage().getEmpty(identifier);
		if (empty == null) {
			returnValue = new Long(0);
			payForwardOrReturn(dhash);
		} else {
			returnValue = getResponse(empty);
			returnHome(dhash);
		}
	}

}
