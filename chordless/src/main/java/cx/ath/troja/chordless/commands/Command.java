/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.commands;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.nja.FriendlyTask;
import cx.ath.troja.nja.FutureValue;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Log;

public abstract class Command implements Serializable {

	public static class MonitorMessage implements Serializable {
		public Identifier source;

		public Identifier destination;

		public String className;

		public Collection<Identifier> regarding;

		public MonitorMessage(Identifier s, Identifier d, String n, Collection<Identifier> r) {
			source = s;
			destination = d;
			className = n;
			regarding = r;
		}
	}

	protected void monitor(Chord chord) {
		if (Chord.doMonitor()) {
			try {
				chord.getExecutorService().getTimeKeeper().begin(Command.class.getName() + ".monitor");
				Chord.sendMonitor(getMonitorMessage(chord.getServerInfo()));
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				chord.getExecutorService().getTimeKeeper().end(Command.class.getName() + ".monitor");
			}
		}
	}

	protected final long createdAt = System.currentTimeMillis();

	protected final Identifier uuid = Identifier.random();

	protected ServerInfo caller;

	private Command() {
	}

	public Command(ServerInfo c) {
		caller = c;
	}

	protected abstract void execute(Chord chord, Sender sender);

	protected Collection<Identifier> getRegarding() {
		Identifier regarding = getRegardingIdentifier();
		if (regarding == null) {
			return null;
		} else {
			Collection<Identifier> returnValue = new LinkedList<Identifier>();
			returnValue.add(regarding);
			return returnValue;
		}
	}

	protected Identifier getRegardingIdentifier() {
		return null;
	}

	public void setCaller(ServerInfo c) {
		caller = c;
	}

	public ServerInfo getCaller() {
		return caller;
	}

	protected MonitorMessage getMonitorMessage(ServerInfo destination) {
		return new MonitorMessage(caller == null ? null : caller.getIdentifier(), destination.getIdentifier(), this.getClass().getName(),
				getRegarding());
	}

	public String toString() {
		return "<" + this.getClass().getName() + " uuid='" + uuid + "'>";
	}

	public int getPriority() {
		return 0;
	}

	public long getCreatedAt() {
		return createdAt;
	}

	public Identifier getUUID() {
		return uuid;
	}

	public int hashCode() {
		return uuid.hashCode();
	}

	public boolean equals(Object o) {
		if (o instanceof Command) {
			return uuid.equals(((Command) o).getUUID());
		} else {
			return false;
		}
	}

	protected ExecutorService getExecutor(Chord chord) {
		return chord.getExecutorService();
	}

	private void enqueue(final Chord chord, final Sender sender, ExecutorService executor) {
		executor.execute(new FriendlyTask("" + this + ".execute(...) running since " + new Date()) {
			public String getDescription() {
				return Command.this.getClass().getName() + ".run";
			}

			public int getPriority() {
				return Command.this.getPriority();
			}

			public void subrun() {
				try {
					Command.this.monitor(chord);
					Command.this.execute(chord, sender);
				} catch (Throwable t) {
					Log.error(this, "Error during " + Command.this + ".run(" + chord + ", " + sender + ")", t);
				}
			}
		});
	}

	public void run(final Chord chord, final Sender sender) {
		if (chord.isShutdown()) {
			FutureValue<Object> future = chord.getStartingFuture();
			if (future == null) {
				sender.send(new RuntimeException("" + chord + " can't run " + this + " since it is currently shut down"));
			} else {
				future.subscribe(new Runnable() {
					public void run() {
						Command.this.run(chord, sender);
					}
				});
			}
		} else {
			if (loggable(this, DEBUG)) {
				debug(this, "" + this + " running on " + chord);
			}
			enqueue(chord, sender, getExecutor(chord));
		}
	}

}
