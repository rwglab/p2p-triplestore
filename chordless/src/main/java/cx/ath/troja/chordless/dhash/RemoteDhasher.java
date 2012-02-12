/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.chordless.dhash.commands.DHashCommand;
import cx.ath.troja.chordless.dhash.commands.RemoteDHashCommand;
import cx.ath.troja.nja.FriendlyScheduledThreadPoolExecutor;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.NamedRunnable;
import cx.ath.troja.nja.SerializedObjectHandler;
import cx.ath.troja.nja.SocketServer;
import cx.ath.troja.nja.SocketServerEventHandler;

public class RemoteDhasher extends Dhasher {

	public interface RemoteDhasherCommand {
		public void run(RemoteDhasher d);
	}

	public class CommandChannel extends SerializedObjectHandler {
		public CommandChannel() {
		}

		@Override
		public void handleClose() {
			RemoteDhasher.this.reconnect();
		}

		@Override
		public void handleObject(final Object o) {
			if (o instanceof RemoteDhasherCommand) {
				RemoteDhasher.this.executorService
						.execute(new NamedRunnable("" + o + ".run(" + RemoteDhasher.this + ") running since " + new Date()) {
							public void subrun() {
								if (RemoteDhasher.this.executorService != null) {
									((RemoteDhasherCommand) o).run(RemoteDhasher.this);
								}
							}
						});
			} else {
				throw new RuntimeException("" + this + " only allows RemoteDHasherCommands, not " + o);
			}
		}
	}

	public static class GetMyAddressCommand extends Command implements RemoteDhasherCommand {
		public InetSocketAddress address;

		public GetMyAddressCommand(ServerInfo source) {
			super(source);
		}

		@Override
		public void run(RemoteDhasher d) {
			synchronized (d) {
				d.setServerInfo(new ServerInfo(new InetSocketAddress(address.getHostName(), d.getSocketServer().getServerSocket().socket()
						.getLocalPort())));
				d.notifyAll();
			}
		}

		@Override
		protected void execute(Chord chord, Sender sender) {
			if (sender instanceof SocketServerEventHandler) {
				SocketAddress remote = ((SocketServerEventHandler) sender).getRemoteAddress();
				if (remote instanceof InetSocketAddress) {
					address = (InetSocketAddress) remote;
					sender.send(this);
				} else {
					throw new RuntimeException("" + this + " only allows InetSocketAddresses as remoteAddress, not " + remote);
				}
			} else {
				throw new RuntimeException("" + this + " only allows NetworkEventHandlers as Sender, not " + sender);
			}
		}
	}

	private SocketServer<CommandChannel> server;

	private CommandChannel handler;

	private ServerInfo serverInfo;

	private Map<Identifier, Receiver> waitingCommands;

	private FriendlyScheduledThreadPoolExecutor executorService;

	private SocketAddress address;

	public RemoteDhasher(SocketAddress a) {
		try {
			address = a;
			waitingCommands = new HashMap<Identifier, Receiver>();
			server = new SocketServer<CommandChannel>().setHandler(CommandChannel.class, this).setAddress(new InetSocketAddress("0.0.0.0", 9898))
					.setAllowLocalPortSearch(true).init();
			handler = null;
			executorService = new FriendlyScheduledThreadPoolExecutor(1);
			synchronized (this) {
				getHandler().send(new GetMyAddressCommand(null));
				this.wait();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String toString() {
		return "<" + getClass().getName() + " address=" + address + ">";
	}

	public void stop() {
		executorService.shutdownNow();
		executorService = null;
		server.close();
	}

	@SuppressWarnings("unchecked")
	private void reconnect() {
		Iterator<Map.Entry<Identifier, Receiver>> iterator = waitingCommands.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Identifier, Receiver> entry = iterator.next();
			entry.getValue().receive(new DHash.ErrorCommand(new RuntimeException("Reconnecting")));
			iterator.remove();
		}
		handler = null;
	}

	private CommandChannel getHandler() {
		try {
			if (handler == null) {
				handler = server.getHandler(address);
			}
			return handler;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public void deliver(RemoteDHashCommand command) {
		Receiver receiver;
		if ((receiver = waitingCommands.remove(command.getUUID())) != null) {
			receiver.receive(command.getNestedCommand());
		} else {
			throw new RuntimeException("Got a returned command that I have no recollection of having sent: " + command);
		}
	}

	protected ServerInfo getServerInfo() {
		return serverInfo;
	}

	public void send(SocketAddress destination, Object o) {
		try {
			server.getHandler(destination).send(o);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void register(final Identifier identifier, final DHashCommand command, final Receiver receiver) {
		try {
			RemoteDHashCommand remoteCommand = new RemoteDHashCommand(identifier, command);
			waitingCommands.put(remoteCommand.getUUID(), receiver);
			getHandler().send(remoteCommand);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private SocketServer getSocketServer() {
		return server;
	}

	private void setServerInfo(ServerInfo s) {
		serverInfo = s;
	}

}