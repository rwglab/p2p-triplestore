/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import static cx.ath.troja.nja.Log.info;

import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.ExecDhasher;
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.nja.Identifier;

public abstract class ExecBase extends ForwardableCommand<Object> {

	protected Identifier identifier;

	protected ServerInfo classLoaderHost;

	public ExecBase(ServerInfo caller, ServerInfo classLoaderHost, Identifier identifier) {
		super(caller);
		this.identifier = identifier;
		this.classLoaderHost = classLoaderHost;
	}

	@Override
	protected Identifier getRegardingIdentifier() {
		return identifier;
	}

	@Override
	public int getPriority() {
		return -10;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " identifier='" + identifier + "' caller='" + caller + "' classLoaderHost='" + classLoaderHost
				+ "' returnValue='" + returnValue + "' done='" + done + "' uuid='" + uuid + "'>";
	}

	public ServerInfo getClassLoaderHost() {
		return classLoaderHost;
	}

	public Identifier getIdentifier() {
		return identifier;
	}

	public Identifier getSource() {
		return getCaller().getIdentifier();
	}

	@Override
	protected void executeHome(DHash dhash) {
		dhash.deliver(this);
	}

	protected void put(ExecDhasher dhasher, Identifier identifier, Object object) {
		dhasher.execPut(identifier, object).get();
	}

	public void done(ExecDhasher dhasher, Object object, boolean tainted) {
		if (tainted) {
			put(dhasher, getIdentifier(), object);
		}
	}

	public void handleException(DHash dhash, Exception e) {
		if (e instanceof NoSuchEntryException) {
			returnValue = e;
			if (((NoSuchEntryException) e).isAuthoritative()) {
				returnHome(dhash);
			} else {
				payForwardOrReturn(dhash);
			}
		} else if (e instanceof RuntimeException) {
			info(this, "Error when running " + this + " on " + identifier + " in " + dhash, e);
			returnValue = e;
			returnHome(dhash);
		}
	}

	@Override
	protected ExecutorService getExecutor(Chord chord) {
		return ((DHash) chord).getExecExecutor();
	}

}
