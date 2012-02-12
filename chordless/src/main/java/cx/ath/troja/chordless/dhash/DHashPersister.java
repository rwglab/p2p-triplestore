/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.commands.CommuteCommand;
import cx.ath.troja.chordless.dhash.commands.DHashCommand;
import cx.ath.troja.chordless.dhash.commands.DelCommand;
import cx.ath.troja.chordless.dhash.commands.EnvoyCommand;
import cx.ath.troja.chordless.dhash.commands.ExecCommand;
import cx.ath.troja.chordless.dhash.commands.GetCommand;
import cx.ath.troja.chordless.dhash.commands.GetCommutationCommand;
import cx.ath.troja.chordless.dhash.commands.GetVersionCommand;
import cx.ath.troja.chordless.dhash.commands.HasCommand;
import cx.ath.troja.chordless.dhash.commands.NextEntryCommand;
import cx.ath.troja.chordless.dhash.commands.PutCommand;
import cx.ath.troja.chordless.dhash.commands.ReplaceCommand;
import cx.ath.troja.chordless.dhash.commands.TakeCommand;
import cx.ath.troja.chordless.dhash.transactions.StartTransactionCommand;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public abstract class DHashPersister implements Persister {

	public static class EntryDelay<T> implements Delay<Map.Entry<Identifier, T>> {
		private Delay<Entry> delay;

		public EntryDelay(Delay<Entry> delay) {
			this.delay = delay;
		}

		public Map.Entry<Identifier, T> get() {
			return get(DEFAULT_TIMEOUT);
		}

		@SuppressWarnings("unchecked")
		public Map.Entry<Identifier, T> get(long timeout) {
			return new AbstractMap.SimpleImmutableEntry<Identifier, T>(delay.get(timeout).getIdentifier(), (T) delay.get(timeout).getValue());
		}
	}

	protected abstract <T> DelayedReceiver<T> getReceiver(String job);

	protected abstract ServerInfo getServerInfo();

	protected ServerInfo getClassLoaderHost() {
		return getServerInfo();
	}

	protected abstract void register(Identifier identifier, DHashCommand command, Receiver receiver);

	@Override
	public <T> Delay<T> take(Object key) {
		DelayedReceiver<T> returnValue = getReceiver("take(" + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new TakeCommand(getServerInfo(), getClassLoaderHost(), identifier), returnValue);
		return returnValue;
	}

	public Delay<Boolean> replace(Object key, Object oldValue, Object newValue) {
		DelayedReceiver<Boolean> returnValue = getReceiver("replace(" + key + ", " + oldValue + ", " + newValue + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new ReplaceCommand(getServerInfo(), getClassLoaderHost(), identifier, oldValue, newValue), returnValue);
		return returnValue;
	}

	public Delay<Boolean> replace(Persistable oldEntry, Persistable newEntry) {
		if (oldEntry.getIdentifier().equals(newEntry.getIdentifier())) {
			return replace(oldEntry.getIdentifier(), oldEntry, newEntry);
		} else {
			throw new IllegalArgumentException("Arguments to #replace must have the same identifiers!");
		}
	}

	@Override
	public Delay<Object> put(Object key, Object value) {
		DelayedReceiver<Object> returnValue = getReceiver("put(" + key + ", " + value + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new PutCommand(getServerInfo(), getClassLoaderHost(), new Entry(identifier, value)), returnValue);
		return returnValue;
	}

	@Override
	public Delay<Object> put(Persistable persistent) {
		return put(persistent.getIdentifier(), persistent);
	}

	@Override
	public Delay<Boolean> del(Object key, Object oldValue) {
		DelayedReceiver<Boolean> returnValue = getReceiver("del(" + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new DelCommand(getServerInfo(), identifier, oldValue), returnValue);
		return returnValue;
	}

	@Override
	public <T> Delay<T> get(Object key) {
		DelayedReceiver<T> returnValue = getReceiver("get(" + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new GetCommand(getServerInfo(), getClassLoaderHost(), identifier), returnValue);
		return returnValue;
	}

	@Override
	public Delay<Long> getVersion(Object key) {
		DelayedReceiver<Long> returnValue = getReceiver("getVersion(" + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new GetVersionCommand(getServerInfo(), identifier), returnValue);
		return returnValue;
	}

	@Override
	public Delay<Long> getCommutation(Object key) {
		DelayedReceiver<Long> returnValue = getReceiver("getCommutation(" + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new GetCommutationCommand(getServerInfo(), identifier), returnValue);
		return returnValue;
	}

	@Override
	public Delay<Boolean> has(Object key) {
		DelayedReceiver<Boolean> returnValue = getReceiver("has(" + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new HasCommand(getServerInfo(), getClassLoaderHost(), identifier), returnValue);
		return returnValue;
	}

	@Override
	public <T> Delay<T> exec(Object key, String methodName, Object... arguments) {
		DelayedReceiver<T> returnValue = getReceiver("exec(" + key + ", " + methodName + ", " + Arrays.asList(arguments) + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new ExecCommand(getServerInfo(), getClassLoaderHost(), identifier, methodName, arguments), returnValue);
		return returnValue;
	}

	@Override
	public <T> Delay<T> commute(Object key, String methodName, Object... arguments) {
		DelayedReceiver<T> returnValue = getReceiver("commute(" + key + ", " + methodName + ", " + Arrays.asList(arguments) + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new CommuteCommand(getServerInfo(), getClassLoaderHost(), identifier, methodName, arguments), returnValue);
		return returnValue;
	}

	@Override
	public <T, V> Delay<V> envoy(Envoy<T> envoy, Object key) {
		DelayedReceiver<V> returnValue = getReceiver("envoy(" + envoy + ", " + key + ")");
		Identifier identifier = Identifier.generate(key);
		register(identifier, new EnvoyCommand(getServerInfo(), getClassLoaderHost(), envoy, identifier), returnValue);
		return returnValue;
	}

	@Override
	public Transaction transaction() {
		DelayedReceiver<Identifier> transactorDelay = getReceiver("transaction()");
		Identifier identifier = Identifier.random();
		register(identifier, new StartTransactionCommand(getServerInfo(), identifier), transactorDelay);
		return new Transaction(this, transactorDelay.get());
	}

	@Override
	public <T> Delay<Map.Entry<Identifier, T>> nextEntry(Identifier previous) {
		final DelayedReceiver<Entry> delay = getReceiver("nextEntry(" + previous + ")");
		register(previous.next(), new NextEntryCommand(getServerInfo(), previous), delay);
		return new EntryDelay<T>(delay);
	}

}
