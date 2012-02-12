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
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Identifier;

public class PutCommand extends MultiplyingCommand<Object> {

	protected Entry entry;

	protected ServerInfo classLoaderHost;

	public PutCommand(ServerInfo caller, ServerInfo classLoaderHost, Entry entry) {
		super(caller);
		this.entry = entry;
		this.classLoaderHost = classLoaderHost;
	}

	@Override
	protected Identifier getRegardingIdentifier() {
		return entry.getIdentifier();
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " entry='" + entry + "' caller='" + caller + "' uuid='" + uuid + "'>";
	}

	@Override
	protected void executeHome(DHash dhash) {
		dhash.deliver(this);
	}

	protected void put(final DHash dhash) {
		dhash.getStorage().put(entry, getCaller().getIdentifier(), new LockingStorage.Returner<Object>() {
			public void call(Object b) {
				PutCommand.this.setReturnValue(null);
				PutCommand.this.returnHome(dhash);
			}
		});
	}

	@Override
	protected void executeAway(final DHash dhash) {
		payForward(dhash);
		try {
			put(dhash);
		} catch (final Cerealizer.ClassNotFoundException e) {
			dhash.getStorage().requestClass(dhash, PutCommand.this.classLoaderHost, e.getClassName(), new Runnable() {
				public void run() {
					PutCommand.this.executeAway(dhash);
				}
			});
		}
	}

}
