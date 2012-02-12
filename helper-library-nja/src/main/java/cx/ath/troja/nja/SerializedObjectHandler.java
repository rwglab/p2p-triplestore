/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.loggable;

/**
 * A class that is extended to create endpoint handlers for the {@link SocketServer} class taking serialized java
 * objects as messages.
 */
public abstract class SerializedObjectHandler extends SocketServerEventHandler {

	private ChunkyStreamUnserializer unserializer = new ChunkyStreamUnserializer() {
		public void handleObject(Object o) {
			SerializedObjectHandler.this.handleObject(o);
		}
	};

	/**
	 * Called when the socket for this handler has produced an object.
	 * 
	 * @param o
	 *            the object from the socket
	 */
	public abstract void handleObject(Object o);

	public void send(Object s) {
		if (loggable(this, DEBUG)) {
			output(DEBUG, "" + this + " sending " + s + " to " + remoteAddress);
		}
		write(unserializer.pack(s));
	}

	public void handleRead(byte[] b) {
		if (loggable(this, DEBUG)) {
			output(DEBUG, "Received " + b.length + " bytes from " + remoteAddress);
		}
		unserializer.handleBytes(b);
	}

}