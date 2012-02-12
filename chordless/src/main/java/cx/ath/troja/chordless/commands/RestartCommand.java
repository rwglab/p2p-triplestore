/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.commands;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.nja.Identifier;

public class RestartCommand extends Command {

	private InetSocketAddress local;

	private SocketAddress bootstrap;

	private Identifier identifier;

	private String serviceName;

	public RestartCommand(ServerInfo caller, InetSocketAddress l, SocketAddress b, Identifier i, String sn) {
		super(caller);
		local = l;
		bootstrap = b;
		identifier = i;
		serviceName = sn;
	}

	protected void stop(Chord chord) {
		chord.stop();
	}

	protected void setup(Chord chord) {
		chord.setLocal(local).setBootstrap(bootstrap).setIdentifier(identifier).setServiceName(serviceName);
	}

	protected void start(Chord chord) {
		chord.start();
	}

	@Override
	public void execute(final Chord chord, final Sender sender) {
		new Thread() {
			public void run() {
				try {
					RestartCommand.this.stop(chord);
					RestartCommand.this.setup(chord);
					RestartCommand.this.start(chord);
					sender.send(Boolean.TRUE);
				} catch (RuntimeException e) {
					sender.send(e);
				} catch (Exception e) {
					sender.send(new RuntimeException(e));
				}
			}
		}.start();
	}

}