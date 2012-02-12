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
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.chordless.dhash.storage.Storage;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Identifier;

public class HasCommand extends ReschedulingCommand<Boolean> {

	public HasCommand(ServerInfo caller, ServerInfo classLoaderHost, Identifier identifier) {
		super(caller, classLoaderHost, identifier);
	}

	@Override
	protected void executeAway(DHash dhash) {
		try {
			dhash.getStorage().getObject(dhash, identifier);
			returnValue = true;
			returnHome(dhash);
		} catch (Cerealizer.ClassNotFoundException e) {
			requestClassAndReschedule(dhash, e.getClassName());
		} catch (Storage.NotPersistExecutorException e) {
			reschedule(dhash);
		} catch (NoSuchEntryException e) {
			if (e.isAuthoritative()) {
				returnValue = false;
				returnHome(dhash);
			} else if (forwardable(dhash)) {
				payForward(dhash);
			} else {
				returnValue = false;
				returnHome(dhash);
			}
		}
	}

}
