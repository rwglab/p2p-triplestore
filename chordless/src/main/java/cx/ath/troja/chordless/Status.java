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
import java.net.InetAddress;
import java.net.SocketAddress;

import cx.ath.troja.nja.TimeKeeper;

public class Status implements Serializable {

	public Class klass;

	public ServerInfo serverInfo;

	public ServerInfo predecessor;

	public ServerInfo[] fingers;

	public ServerInfo[] successors;

	public TimeKeeper executorTimer;

	public TimeKeeper selectorTimer;

	public int queueSize;

	public InetAddress localInetAddress;

	public int localPort;

	public SocketAddress bootstrapAddress;

	public String logProperties;

	public String serviceName;

	public String toString() {
		return "" + serverInfo + ", " + executorTimer.load();
	}

}