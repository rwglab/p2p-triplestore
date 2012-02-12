/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;

import java.net.ConnectException;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;

public abstract class ForwardableCommand<T> extends ReturnValueCommand<T> {

	protected int hops;

	public ForwardableCommand(ServerInfo c) {
		super(c);
		hops = 1;
	}

	protected void payForward(DHash dhash) {
		if (forwardable(dhash)) {
			hops++;
			try {
				dhash.send(this, dhash.getSuccessor().getAddress());
			} catch (ConnectException e) {
				hops--;
				if (loggable(this, DEBUG))
					debug(this, "Error while trying to send " + this + " to " + dhash.getSuccessor());
				dhash.shiftSuccessors();
				payForward(dhash);
			}
		}
	}

	protected boolean forwardable(DHash dhash) {
		return hops < dhash.getCopies();
	}

	protected void payForwardOrReturn(DHash dhash) {
		if (forwardable(dhash)) {
			payForward(dhash);
		} else {
			returnHome(dhash);
		}
	}

}
