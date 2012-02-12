/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.tools;

import cx.ath.troja.chordless.commands.PingCommand;
import cx.ath.troja.chordless.commands.PongCommand;

public class NetworkStressTester {

	private CommandSender sender;

	public static void main(String[] arguments) {
		if (arguments.length < 3) {
			System.err.println("Usage: NetworkStressTester HOST PORT N");
			System.exit(1);
		} else {
			new NetworkStressTester(arguments[0], Integer.parseInt(arguments[1])).stress(Long.parseLong(arguments[2]));
		}
	}

	public NetworkStressTester(String host, int port) {
		sender = new CommandSender(host, port);
	}

	public void stress(long n) {
		for (long i = 0; i < n; i++) {
			PingCommand ping = new PingCommand(null);
			Object s = sender.send(ping);
			if (((PongCommand) s).getSequence() != ping.getSequence()) {
				throw new RuntimeException("Expected a PongCommand with sequence " + ping.getSequence() + ", but got " + s);
			}
			System.out.print(".");
		}
		System.out.println("OK");
	}

}
