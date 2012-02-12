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
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.FriendlyTask;
import cx.ath.troja.nja.Identifier;

public class EnvoyCommand extends ExecBase {

	private static class EnvoyBackend implements Persister.EnvoyBackend {
		private EnvoyCommand command;

		private DHash dhash;

		private Persister.Envoy<?> envoy;

		public EnvoyBackend(EnvoyCommand command, DHash dhash, Persister.Envoy<?> envoy) {
			this.command = command;
			this.dhash = dhash;
			this.envoy = envoy;
		}

		public void returnHome(Object value) {
			final EnvoyCommand cmd = (EnvoyCommand) Cerealizer.unpack(Cerealizer.pack(command));
			cmd.done = true;
			cmd.setReturnValue(value);
			dhash.getExecExecutor().execute(new FriendlyTask("" + this + ".returnHome(...) running since " + new Date()) {
				public String getDescription() {
					return cmd.getClass().getName() + ".returnHome";
				}

				public int getPriority() {
					return cmd.getPriority();
				}

				public void subrun() {
					cmd.returnHome(dhash);
				}
			});
		}

		public void redirect(final Object key) {
			final EnvoyCommand cmd = (EnvoyCommand) Cerealizer.unpack(Cerealizer.pack(command));
			cmd.setIdentifier(Identifier.generate(key));
			cmd.setEnvoy(envoy);
			dhash.getExecExecutor().execute(new FriendlyTask("" + this + ".redirect(...) running since " + new Date()) {
				public String getDescription() {
					return cmd.getClass().getName() + ".redirect";
				}

				public int getPriority() {
					return cmd.getPriority();
				}

				public void subrun() {
					dhash.sendToSuccessor(cmd.getIdentifier(), cmd);
				}
			});
		}
	}

	protected byte[] serializedEnvoy;

	public EnvoyCommand(ServerInfo caller, ServerInfo classLoaderHost, Persister.Envoy<?> envoy, Identifier identifier) {
		super(caller, classLoaderHost, identifier);
		setEnvoy(envoy);
	}

	public Persister.Envoy<?> getEnvoy(ClassLoader cl) {
		return (Persister.Envoy<?>) Cerealizer.unpack(serializedEnvoy, cl);
	}

	public void setIdentifier(Identifier i) {
		identifier = i;
	}

	public void setEnvoy(Persister.Envoy<?> e) {
		serializedEnvoy = Cerealizer.pack(e);
	}

	@Override
	protected void executeAway(final DHash dhash) {
		dhash.getStorage().envoy(dhash, this);
	}

	public void handle(final DHash dhash, Object object, ClassLoader cl) {
		Persister.Envoy<?> envoy = getEnvoy(cl);
		envoy.setBackend(new EnvoyBackend(this, dhash, envoy));
		envoy.handleWithCast(object);
	}

}
