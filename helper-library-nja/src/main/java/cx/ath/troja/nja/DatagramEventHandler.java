/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.net.DatagramPacket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;

/**
 * A class that is extended to create endpoint handlers for the {@link SocketServer} class.
 */
public abstract class DatagramEventHandler extends NetworkEventHandler {

	/**
	 * Enqueues bytes to the write queue of this handler and notifies the SelectorThread that it wants to write.
	 * 
	 * @param bytes
	 *            the data to write
	 */
	public void write(DatagramPacket packet) {
		try {
			getWriteQueue().add(packet);
			SelectorThread.getInstance().interestOps(selectionKey, selectionKey.interestOps() | SelectionKey.OP_WRITE);
		} catch (CancelledKeyException e) {
			close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Closes this handler, cancels its java.nio.channels#SelectionKey and closes its java.nio.channels#SocketChannel.
	 */
	@Override
	public void close() {
		try {
			super.close();
			handleClose();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Called when the socket for this handler has been closed by the remote end.
	 */
	public void handleClose() {
	}

}