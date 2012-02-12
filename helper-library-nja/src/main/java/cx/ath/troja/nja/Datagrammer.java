/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Future;

public class Datagrammer {

	private SocketAddress localAddress;

	private NetworkEventHandler handler;

	private SelectionKey selectionKey;

	private DatagramChannel datagramChannel;

	private boolean initialized;

	public Datagrammer() {
		this(null);
	}

	public Datagrammer(SocketAddress localAddress) {
		this.localAddress = localAddress;
		initialized = false;
		handler = null;
		selectionKey = null;
		datagramChannel = null;
	}

	public Datagrammer setHandler(NetworkEventHandler handler) {
		this.handler = handler;
		if (selectionKey != null) {
			selectionKey.attach(handler);
		}
		return this;
	}

	public NetworkEventHandler getHandler() {
		return handler;
	}

	public void close() {
		datagramChannel.socket().close();
		selectionKey.cancel();
	}

	public DatagramChannel getChannel() {
		return datagramChannel;
	}

	public Datagrammer init() {
		try {
			if (initialized == false) {
				datagramChannel = DatagramChannel.open();
				datagramChannel.configureBlocking(false);
				if (localAddress != null) {
					datagramChannel.socket().setReuseAddress(true);
					datagramChannel.socket().bind(localAddress);
				}
				Future<SelectionKey> futureKey = SelectorThread.getInstance().register(datagramChannel);
				selectionKey = futureKey.get();
				handler.setSelectionKey(selectionKey);
				selectionKey.attach(handler);
				initialized = true;
			}
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}