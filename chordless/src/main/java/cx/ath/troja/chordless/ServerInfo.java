/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless;

import java.io.Serializable;
import java.net.SocketAddress;

import cx.ath.troja.nja.Identifier;

public class ServerInfo implements Comparable<ServerInfo>, Serializable {

	private static final long serialVersionUID = 1L;

	private Identifier identifier;

	private SocketAddress address;

	public ServerInfo(SocketAddress a, Identifier i) {
		address = a;
		identifier = i;
	}

	public ServerInfo(SocketAddress a) {
		address = a;
		identifier = Identifier.generate(a);
	}

	public Identifier getIdentifier() {
		return identifier;
	}

	public SocketAddress getAddress() {
		return address;
	}

	public String toString() {
		return "<" + this.getClass().getName() + " identifier='" + identifier.toString() + "' address='" + address + "'>";
	}

	public String toShortString() {
		return identifier.toString() + ":" + address;
	}

	public boolean equals(Object o) {
		if (o instanceof ServerInfo) {
			ServerInfo other = (ServerInfo) o;
			return other.getIdentifier().equals(getIdentifier()) && other.getAddress().equals(getAddress());
		} else {
			return false;
		}
	}

	public int compareTo(ServerInfo other) {
		int returnValue = identifier.compareTo(other.getIdentifier());
		if (returnValue == 0 && !address.equals(other.getAddress())) {
			throw new RuntimeException("Another node has the same Identifier but not the same address!");
		}
		return returnValue;
	}

}