/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.tools;

import java.net.ConnectException;
import java.net.InetSocketAddress;

import cx.ath.troja.chordless.commands.ScriptExecutionCommand;
import cx.ath.troja.nja.RuntimeArguments;

public class Evaluator {

	public static void main(String[] args) throws ConnectException {
		RuntimeArguments arguments = new RuntimeArguments(args);
		if (arguments.get("host") == null || arguments.get("port") == null || arguments.get("command") == null) {
			System.out.println("Usage: ControlFrame host=HOST port=PORT command=COMMAND");
		} else {
			RemoteChordProxy proxy = new RemoteChordProxy(new InetSocketAddress(arguments.get("host"), Integer.parseInt(arguments.get("port"))));
			System.out.println(proxy.run(new ScriptExecutionCommand(null, arguments.get("command"))));
		}
	}

}