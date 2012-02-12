/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.tools;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.nja.ChunkyStreamUnserializer;

public class CommandSender {

	private Socket socket;

	private ObjectOutputStream out;

	private ObjectInputStream in;

	public CommandSender(String host, int port) {
		try {
			socket = new Socket(host, port);
			out = null;
			in = null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void abandon(Command command) {
		try {
			socket.getOutputStream().write(ChunkyStreamUnserializer.pack(command));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Object receive() {
		try {
			byte[] spam = new byte[4];
			socket.getInputStream().read(spam);
			return new ObjectInputStream(socket.getInputStream()).readObject();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Object send(Command command) {
		try {
			abandon(command);
			return receive();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}