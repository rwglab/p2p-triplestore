/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import java.util.Arrays;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.commands.CommutePutCommand;
import cx.ath.troja.chordless.dhash.commands.ExecPutCommand;
import cx.ath.troja.chordless.dhash.commands.ReturnValueCommand;
import cx.ath.troja.chordless.dhash.storage.LockingStorage;
import cx.ath.troja.chordless.dhash.transactions.CommitCommand;
import cx.ath.troja.chordless.dhash.transactions.PrepareCommand;
import cx.ath.troja.chordless.dhash.transactions.Transactor;
import cx.ath.troja.chordless.dhash.transactions.TryPrepareCommand;
import cx.ath.troja.chordless.dhash.transactions.UnlockCommand;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Identifier;

/**
 * Used to provide a Dhasher to Persistent objects when they are executed within a database node.
 */
public class ExecDhasher extends Dhasher implements Transactor {

	public class ClassLoadingReturnValueCommandFuture<T> extends Dhasher.ReturnValueCommandFuture<T> {
		public ClassLoadingReturnValueCommandFuture(String cause) {
			super(cause);
		}

		@Override
		public void receive(final ReturnValueCommand<T> command) {
			try {
				receiverFuture.set(command.getReturnValue(ExecDhasher.this.getDHash().getStorage().getClassLoader()));
			} catch (final Cerealizer.ClassNotFoundException e) {
				ExecDhasher.this.getDHash().getStorage()
						.requestClass(ExecDhasher.this.getDHash(), ExecDhasher.this.classLoaderHost, e.getClassName(), new Runnable() {
							public void run() {
								ClassLoadingReturnValueCommandFuture.this.receive(command);
							}
						});
			}
		}
	}

	private ServerInfo classLoaderHost;

	public ExecDhasher(DHash d, ServerInfo l) {
		super(d);
		classLoaderHost = l;
	}

	@Override
	public <T> DelayedReceiver<T> getReceiver(String job) {
		return new ClassLoadingReturnValueCommandFuture<T>(job);
	}

	@Override
	protected ServerInfo getClassLoaderHost() {
		return classLoaderHost;
	}

	@Override
	public ClassLoader getClassLoader() {
		return getDHash().getStorage().getClassLoader();
	}

	@Override
	public Delay<? extends Object> abort(Identifier i, Identifier source) {
		DelayedReceiver<Boolean> returnValue = getReceiver("abort(" + i + ", " + source + ")");
		register(i, new UnlockCommand(getServerInfo(), i, source), returnValue);
		return returnValue;
	}

	@Override
	public Delay<LockingStorage.LockResponse> prepare(Identifier i, long version, Long commutation, Identifier source) {
		DelayedReceiver<LockingStorage.LockResponse> returnValue = getReceiver("prepare(" + i + ", " + version + ", " + commutation + ", " + source
				+ ")");
		register(i, new PrepareCommand(getServerInfo(), i, version, commutation, source), returnValue);
		return returnValue;
	}

	@Override
	public Delay<LockingStorage.LockResponse> tryPrepare(Identifier i, long version, Long commutation, Identifier source) {
		DelayedReceiver<LockingStorage.LockResponse> returnValue = getReceiver("tryPrepare(" + i + ", " + version + ", " + commutation + ", "
				+ source + ")");
		register(i, new TryPrepareCommand(getServerInfo(), i, version, commutation, source), returnValue);
		return returnValue;
	}

	@Override
	public Delay<? extends Object> update(Entry e, Identifier source) {
		DelayedReceiver<Object> returnValue = getReceiver("update(" + e + ", " + source + ")");
		register(e.getIdentifier(), new CommitCommand(getServerInfo(), getClassLoaderHost(), e, source, false), returnValue);
		return returnValue;
	}

	@Override
	public Delay<? extends Object> update(Entry e, Identifier source, String methodName, Object... arguments) {
		DelayedReceiver<Object> returnValue = getReceiver("update(" + e + ", " + source + ", " + methodName + ", " + Arrays.asList(arguments) + ")");
		register(e.getIdentifier(), new CommitCommand(getServerInfo(), getClassLoaderHost(), e, source, true), returnValue);
		return returnValue;
	}

	public Delay<? extends Object> execPut(Identifier key, Object value) {
		DelayedReceiver<Object> returnValue = getReceiver("execPut(" + key + ", " + value + ")");
		register(key, new ExecPutCommand(getServerInfo(), getClassLoaderHost(), new Entry(key, value)), returnValue);
		return returnValue;
	}

	public Delay<? extends Object> commutePut(Identifier key, Object value) {
		DelayedReceiver<Object> returnValue = getReceiver("commutePut(" + key + ", " + value + ")");
		register(key, new CommutePutCommand(getServerInfo(), getClassLoaderHost(), new Entry(key, value)), returnValue);
		return returnValue;
	}

}
