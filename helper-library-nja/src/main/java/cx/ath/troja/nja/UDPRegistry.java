/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.warn;

import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.MessageFormat;

public class UDPRegistry {

	public static void main(String[] arguments) {
		SelectorThread.getInstance().setDaemon(false);
		int port = arguments.length > 0 ? Integer.parseInt(arguments[0]) : 5959;
		new UDPRegistry(new InetSocketAddress("0.0.0.0", port));
	}

	private class UDPRegistryHandler extends DatagramEventHandler {
		@Override
		public void handleRead(byte[] bytes, SocketAddress source) {
			try {
				String message = new String(bytes);
				if (message.trim().equals(UDPHole.introspectionMessage)) {
					InetSocketAddress inetSource = (InetSocketAddress) source;
					String response = MessageFormat.format(UDPHole.introspectionResponseMessageFormat,
							inetSource.getHostName() + ":" + inetSource.getPort());
					write(new DatagramPacket(response.getBytes(), response.getBytes().length, source));
				} else {
					System.out.println("got " + bytes.length + " ping from " + source);
					write(new DatagramPacket(new byte[1], 1, source));
				}
			} catch (Exception e) {
				warn(this, "While trying to handle read of " + bytes + " from " + source, e);
				throw new RuntimeException(e);
			}
		}
	}

	private Datagrammer datagrammer;

	public UDPRegistry(InetSocketAddress address) {
		datagrammer = new Datagrammer(address).setHandler(new UDPRegistryHandler()).init();
	}
}