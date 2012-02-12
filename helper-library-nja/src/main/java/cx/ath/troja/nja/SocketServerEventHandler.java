/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.loggable;

import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Future;
import java.util.logging.Level;

/**
 * A class that is extended to create endpoint handlers for the {@link SocketServer} class.
 */
public abstract class SocketServerEventHandler extends NetworkEventHandler {

	private SocketServer server;

	protected SocketAddress remoteAddress;

	protected void output(Level level, String message, Throwable t) {
		if (loggable(this, level)) {
			Log.output(this, level, message, t);
		}
		if (loggable(connectionId(), level)) {
			Log.output(connectionId(), level, message, t);
		}
	}

	protected void output(Level level, String message) {
		output(level, message, null);
	}

	/**
	 * Get something unique for this connection.
	 */
	public String connectionId() {
		if (selectionKey == null) {
			return "null.null";
		} else {
			Socket socket = ((SocketChannel) selectionKey.channel()).socket();
			return "" + socket.getLocalPort() + "." + socket.getPort();
		}
	}

	public SocketAddress getRemoteAddress() {
		return remoteAddress;
	}

	/**
	 * Sets the SocketServer of this handler.
	 * 
	 * @param s
	 *            the {@link SocketServer} using this handler
	 * @return this NetworkEventHandler, to call more methods on
	 */
	protected SocketServerEventHandler setServer(SocketServer s) {
		server = s;
		return this;
	}

	/**
	 * Will connect this handler to an address.
	 * 
	 * Use EITHER this method or setSelectionKey, not both.
	 * 
	 * @param a
	 *            the address to connect to
	 */
	@SuppressWarnings("unchecked")
	protected NetworkEventHandler connect(SocketAddress address) throws ConnectException {
		try {
			remoteAddress = address;
			SocketChannel newSocket = SocketChannel.open(address);
			newSocket.socket().setKeepAlive(true);
			newSocket.configureBlocking(false);

			Future<SelectionKey> futureKey = SelectorThread.getInstance().register(newSocket);
			selectionKey = futureKey.get();

			server.getHandlerByAddress().put(address, this);
			selectionKey.attach(this);

			return this;
		} catch (ConnectException e) {
			throw e;
		} catch (ClosedByInterruptException e) {
			throw new ConnectException("" + e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Sets the SelectionKey of this handler.
	 * 
	 * Use EITHER this method or connect, not both.
	 * 
	 * @param k
	 *            the java.nio.channels#SelectionKey attaching this handler
	 * @return this NetworkEventHandler, to call more methods on
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected SocketServerEventHandler setSelectionKey(SelectionKey k) {
		try {
			super.setSelectionKey(k);
			Socket socket = ((SocketChannel) selectionKey.channel()).socket();
			remoteAddress = socket.getRemoteSocketAddress();
			socket.setKeepAlive(true);
			server.getHandlerByAddress().put(((SocketChannel) selectionKey.channel()).socket().getRemoteSocketAddress(), this);
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Closes this handler, removes it from the map of handlers by address and calls handleClose.
	 */
	@Override
	public void close() {
		try {
			server.getHandlerByAddress().remove(remoteAddress);
			super.close();
			handleClose();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Called when the socket for this handler is first connected.
	 */
	public void handleConnect() {
	}

	/**
	 * Called when the socket for this handler has been closed by the remote end.
	 */
	public void handleClose() {
	}

	/**
	 * Enqueues bytes to the write queue of this handler and notifies the SelectorThread that it wants to write.
	 * 
	 * @param bytes
	 *            the data to write
	 */
	public void write(byte[] bytes) {
		try {
			getWriteQueue().add(ByteBuffer.wrap(bytes));
			SelectorThread.getInstance().interestOps(selectionKey, selectionKey.interestOps() | SelectionKey.OP_WRITE);
		} catch (CancelledKeyException e) {
			close();
		}
	}

	/**
	 * Called when the socket for this handler has produced some data.
	 * 
	 * @param bytes
	 *            the data produced by the socket
	 */
	public abstract void handleRead(byte[] bytes);

	public void handleRead(byte[] bytes, SocketAddress source) {
		handleRead(bytes);
	}

}