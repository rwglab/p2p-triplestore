/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */
/*
 * ControlFrame.java Created on January 19, 2009, 3:23 PM
 */

package cx.ath.troja.chordless.tools;

import java.io.ObjectInputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.nja.ChunkyStreamUnserializer;

public class RemoteChordProxy implements ChordProxy {
	private Socket socket;

	public RemoteChordProxy(InetSocketAddress address) throws ConnectException {
		connect(address);
	}

	private void connect(InetSocketAddress address) throws ConnectException {
		int deciWait = 10;
		for (int i = 0; i < deciWait; i++) {
			try {
				socket = new Socket(address.getHostName(), address.getPort());
				break;
			} catch (Exception e) {
				if (i == deciWait - 1) {
					if (e instanceof ConnectException) {
						throw (ConnectException) e;
					} else {
						throw new RuntimeException(e);
					}
				} else {
					try {
						Thread.sleep(100);
					} catch (Exception e2) {
						throw new RuntimeException(e2);
					}
				}
			}
		}
	}

	public synchronized Object run(Command command) {
		try {
			socket.getOutputStream().write(ChunkyStreamUnserializer.pack(command));
			byte[] header = new byte[4];
			socket.getInputStream().read(header);
			return new ObjectInputStream(socket.getInputStream()).readObject();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
