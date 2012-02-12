/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.transactions;

import java.util.Collection;
import java.util.LinkedList;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.commands.MultiplyingCommand;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.nja.Identifier;

public class TryPrepareCommand extends MultiplyingCommand<Object> {

	protected long version;

	protected Identifier identifier;

	protected Identifier source;

	protected Long commutation;

	public TryPrepareCommand(ServerInfo c, Identifier i, long v, Long com, Identifier s) {
		super(c);
		identifier = i;
		source = s;
		version = v;
		commutation = com;
	}

	@Override
	public int getPriority() {
		return -50;
	}

	@Override
	protected Collection<Identifier> getRegarding() {
		Collection<Identifier> returnValue = new LinkedList<Identifier>();
		returnValue.add(identifier);
		returnValue.add(source);
		return returnValue;
	}

	@Override
	protected void executeHome(DHash dhash) {
		dhash.deliver(this);
	}

	@Override
	protected void executeAway(DHash dhash) {
		payForward(dhash);
		LockingStorage.LockResponse response = dhash.getStorage().tryLock(identifier, version, commutation, source);
		setReturnValue(response);
		returnHome(dhash);
	}
}
