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
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * A class that is extended to create endpoint handlers for the {@link SocketServer} class.
 */
public abstract class NetworkEventHandler {

	/**
	 * The SelectionKey that is the root cause of this handler.
	 */
	protected SelectionKey selectionKey;

	private Deque<Object> writeQueue = new LinkedBlockingDeque<Object>();

	/**
	 * Gets the write queue for this handler
	 * 
	 * @return the write queue for this handler
	 */
	protected Deque<Object> getWriteQueue() {
		return writeQueue;
	}

	/**
	 * Sets the SelectionKey of this handler.
	 * 
	 * @param k
	 *            the java.nio.channels#SelectionKey attaching this handler
	 * @return this NetworkEventHandler, to call more methods on
	 */
	@SuppressWarnings("unchecked")
	protected NetworkEventHandler setSelectionKey(SelectionKey k) {
		selectionKey = k;
		return this;
	}

	/**
	 * Closes this handler, cancels its java.nio.channels#SelectionKey and closes its
	 * java.nio.channels#SelectableChannel.
	 */
	public void close() {
		try {
			selectionKey.cancel();
			((SelectableChannel) selectionKey.channel()).close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Called when the socket for this handler has produced some data.
	 * 
	 * @param bytes
	 *            the data produced by the socket
	 * @param source
	 *            the source of these bytes
	 */
	public abstract void handleRead(byte[] bytes, SocketAddress source);

}