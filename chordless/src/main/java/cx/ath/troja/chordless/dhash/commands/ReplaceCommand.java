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
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.chordless.dhash.storage.Storage;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Identifier;

public class ReplaceCommand extends TakeCommand {

	private Entry oldEntry;

	private Entry newEntry;

	public ReplaceCommand(ServerInfo caller, ServerInfo classLoaderHost, Identifier identifier, Object oldValue, Object newValue) {
		super(caller, classLoaderHost, identifier);
		this.oldEntry = new Entry(identifier, oldValue);
		this.newEntry = new Entry(identifier, newValue);
		this.returnValue = Boolean.FALSE;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " identifier=" + identifier + " caller='" + caller + "' uuid='" + uuid + "'>";
	}

	@Override
	protected void executeAway(final DHash dhash) {
		if (primary) {
			try {
				Object existing = null;
				try {
					existing = dhash.getStorage().getObject(dhash, identifier);
				} catch (NoSuchEntryException e) {
					if (e.isAuthoritative()) {
						existing = null;
					} else {
						if (forwardable(dhash)) {
							throw e;
						} else {
							existing = null;
						}
					}
				}
				primary = false;
				if ((existing == null && oldEntry == null) || (existing != null)) {
					payForward(dhash);
					scheduleUpdate(dhash, oldEntry, newEntry, getCaller().getIdentifier(), new LockingStorage.Returner<Object>() {
						public void call(Object b) {
							ReplaceCommand.this.setReturnValue(true);
							ReplaceCommand.this.returnHome(dhash);
						}
					});
				} else {
					returnHome(dhash);
				}
			} catch (Storage.NotPersistExecutorException e) {
				reschedule(dhash);
			} catch (Cerealizer.ClassNotFoundException e) {
				requestClassAndReschedule(dhash, e.getClassName());
			} catch (NoSuchEntryException e) {
				if (e.isAuthoritative()) {
					throw new RuntimeException("Should not be able to get this exception here!", e);
				} else {
					payForwardOrReturn(dhash);
				}
			}
		} else {
			if (forwardable(dhash)) {
				payForward(dhash);
			}
			scheduleUpdate(dhash, oldEntry, newEntry, getCaller().getIdentifier(), new LockingStorage.Returner<Object>() {
				public void call(Object b) {
				}
			});
		}
	}

}
