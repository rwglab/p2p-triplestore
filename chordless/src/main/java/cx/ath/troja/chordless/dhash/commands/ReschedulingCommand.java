/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2011 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import java.util.Date;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.nja.FriendlyTask;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Log;

public abstract class ReschedulingCommand<K> extends ForwardableCommand<K> {

	protected Identifier identifier;

	protected ServerInfo classLoaderHost;

	public ReschedulingCommand(ServerInfo caller, ServerInfo classLoaderHost, Identifier identifier) {
		super(caller);
		this.identifier = identifier;
		this.classLoaderHost = classLoaderHost;
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
	protected ExecutorService getExecutor(Chord chord) {
		return ((DHash) chord).getExecExecutor();
	}

	protected void requestClassAndReschedule(final DHash dhash, String className) {
		dhash.getStorage().requestClass(dhash, ReschedulingCommand.this.classLoaderHost, className, new Runnable() {
			public void run() {
				ReschedulingCommand.this.executeAway(dhash);
			}
		});
	}

	protected void reschedule(final DHash dhash) {
		dhash.getPersistExecutor().execute(new FriendlyTask("" + this + ".executeAway(...) running since " + new Date()) {
			public String getDescription() {
				return ReschedulingCommand.class.getName() + ".run";
			}

			public int getPriority() {
				return ReschedulingCommand.this.getPriority();
			}

			public void subrun() {
				try {
					ReschedulingCommand.this.executeAway(dhash);
				} catch (Throwable t) {
					Log.error(this, "Error during " + ReschedulingCommand.this + ".executeAway(" + dhash + ")", t);
				}
			}
		});
	}

}
