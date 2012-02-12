/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import java.util.Date;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.chordless.dhash.storage.Storage;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.FriendlyTask;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Log;

public class TakeCommand extends GetCommand {

	protected boolean primary;

	public TakeCommand(ServerInfo caller, ServerInfo classLoaderHost, Identifier identifier) {
		super(caller, classLoaderHost, identifier);
		primary = true;
	}

	private Entry nullEntry() {
		return new Entry(identifier, null, getCreatedAt(), 0, 0, 0, "null", null);
	}

	protected void schedulePut(final DHash dhash, final Entry entry, final Identifier caller, final LockingStorage.Returner<Object> returner) {
		dhash.getPersistExecutor().execute(new FriendlyTask("" + this + ".schedulePut(...) running since " + new Date()) {
			public String getDescription() {
				return TakeCommand.this.getClass().getName() + ".schedulePut";
			}

			public int getPriority() {
				return TakeCommand.this.getPriority();
			}

			public void subrun() {
				try {
					dhash.getStorage().put(entry, caller, returner);
				} catch (Throwable t) {
					Log.error(this, "Error during " + TakeCommand.this + ".schedulePut(...)");
				}
			}

		});
	}

	protected void scheduleUpdate(final DHash dhash, final Entry oldEntry, final Entry newEntry, final Identifier caller,
			final LockingStorage.Returner<Object> returner) {
		dhash.getPersistExecutor().execute(new FriendlyTask("" + this + "..scheduleUpdate(...) running since " + new Date()) {
			public String getDescription() {
				return TakeCommand.this.getClass().getName() + ".scheduleUpdate";
			}

			public int getPriority() {
				return TakeCommand.this.getPriority();
			}

			public void subrun() {
				try {
					dhash.getStorage().update(oldEntry, newEntry, caller, returner);
				} catch (Throwable t) {
					Log.error(this, "Error during " + TakeCommand.this + ".scheduleUpdate(...)");
				}
			}

		});
	}

	@Override
	protected void executeAway(final DHash dhash) {
		if (primary) {
			try {
				returnValue = dhash.getStorage().getObject(dhash, identifier);
				primary = false;
				payForward(dhash);
				schedulePut(dhash, nullEntry(), getCaller().getIdentifier(), new LockingStorage.Returner<Object>() {
					public void call(Object b) {
						TakeCommand.this.returnHome(dhash);
					}
				});
			} catch (Cerealizer.ClassNotFoundException e) {
				requestClassAndReschedule(dhash, e.getClassName());
			} catch (Storage.NotPersistExecutorException e) {
				reschedule(dhash);
			} catch (NoSuchEntryException e) {
				if (e.isAuthoritative()) {
					returnHome(dhash);
				} else {
					payForwardOrReturn(dhash);
				}
			}
		} else {
			if (forwardable(dhash)) {
				payForward(dhash);
			}
			schedulePut(dhash, nullEntry(), getCaller().getIdentifier(), new LockingStorage.Returner<Object>() {
				public void call(Object b) {
				}
			});
		}
	}

}
