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

public class ExecPutCommand extends PutCommand {

	public ExecPutCommand(ServerInfo caller, ServerInfo classLoaderHost, Entry entry) {
		super(caller, classLoaderHost, entry);
	}

	@Override
	protected void put(final DHash dhash) {
		dhash.getStorage().execPut(entry, getCaller().getIdentifier(), new LockingStorage.Returner<Object>() {
			public void call(Object b) {
				ExecPutCommand.this.setReturnValue(null);
				ExecPutCommand.this.returnHome(dhash);
			}
		});
	}

}
