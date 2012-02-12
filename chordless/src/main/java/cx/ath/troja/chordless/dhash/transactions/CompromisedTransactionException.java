/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.transactions;

import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.nja.Identifier;

public class CompromisedTransactionException extends RuntimeException {

	private String message;

	public String getMessage() {
		return message;
	}

	public CompromisedTransactionException(String s) {
		message = s;
	}

	public CompromisedTransactionException(String s, Throwable t) {
		super(t);
		message = s;
	}

	private String defaultMessage(Identifier i, Long wantedVersion, Long wantedCommutation) {
		return ("Unable to lock " + i + " in version " + wantedVersion + " and commutation " + wantedCommutation);
	}

	public CompromisedTransactionException(LockingStorage.LockResponse resp, Identifier i, Long wantedVersion, Long wantedCommutation) {
		if (resp.code == LockingStorage.LockResponse.LOCK_ALREADY_LOCKED) {
			message = defaultMessage(i, wantedVersion, wantedCommutation) + " due to " + resp
					+ ". This should not be a reason to fail, so this is bad.";
		} else if (resp.code == LockingStorage.LockResponse.LOCK_SUCCESS) {
			message = defaultMessage(i, wantedVersion, wantedCommutation) + " due to " + resp + ". This is weird, because it claims to be a success?";
		} else if (resp.code == LockingStorage.LockResponse.LOCK_OUTDATED) {
			message = defaultMessage(i, wantedVersion, wantedCommutation) + " due to " + resp;
		} else {
			throw new RuntimeException("Unknown code in " + resp);
		}
	}

}
