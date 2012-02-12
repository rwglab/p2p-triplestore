/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.tools;

import java.net.InetSocketAddress;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.Receiver;
import cx.ath.troja.chordless.dhash.commands.DHashCommand;
import cx.ath.troja.chordless.dhash.commands.ExecCommand;
import cx.ath.troja.chordless.dhash.commands.PutCommand;
import cx.ath.troja.nja.Identifier;

public class DatabaseStressTester {

	private static class Exec extends Command {
		private Identifier identifier;

		public Exec(Identifier i) {
			super(null);
			identifier = i;
		}

		protected void execute(Chord chord, final Sender sender) {
			DHash dhash = (DHash) chord;
			DHashCommand command = new ExecCommand(dhash.getServerInfo(), null, identifier, "toString");
			dhash.registerAndSendToSuccessor(identifier, command, new Receiver<ExecCommand>() {
				public long getTimeout() {
					return DEFAULT_TIMEOUT;
				}

				public void receive(ExecCommand command) {
					sender.send(command.getReturnValue());
				}
			});
		}
	}

	private static class Put extends Command {
		private Entry entry;

		public Put(Entry e) {
			super(null);
			entry = e;
		}

		protected void execute(Chord chord, final Sender sender) {
			DHash dhash = (DHash) chord;
			DHashCommand command = new PutCommand(dhash.getServerInfo(), null, entry);
			dhash.registerAndSendToSuccessor(entry.getIdentifier(), command, new Receiver<PutCommand>() {
				public long getTimeout() {
					return DEFAULT_TIMEOUT;
				}

				public void receive(PutCommand command) {
					sender.send(command.getReturnValue());
				}
			});
		}
	}

	private CommandSender sender;

	private ServerInfo fakeInfo;

	private boolean abandon;

	public static void main(String[] arguments) throws Exception {
		if (arguments.length < 6) {
			System.err.println("Usage: DatabaseStressTester HOST PORT N RATE ABANDON(t|f) TYPE(put|exec)");
			System.exit(1);
		} else {
			new DatabaseStressTester(arguments[0], Integer.parseInt(arguments[1]), arguments[4].equals("t")).stress(Long.parseLong(arguments[2]),
					Integer.parseInt(arguments[3]), arguments[5]);
		}
	}

	public DatabaseStressTester(String host, int port, boolean a) throws Exception {
		abandon = a;
		sender = new CommandSender(host, port);
		fakeInfo = new ServerInfo(new InetSocketAddress(host, port), Identifier.generate("fake!"));
	}

	public void stress(long n, int rate, String type) throws Exception {
		long t = System.currentTimeMillis();
		int middle = 0;
		long mtime = System.currentTimeMillis();
		int middlesize = 200;
		if (type.equals("exec")) {
			sender.send(new Put(new Entry(new Identifier(1), "blapp")));
		}
		for (long i = 0; i < n; i++) {
			Command command = null;
			if (type.equals("put")) {
				command = new Put(new Entry(Identifier.generate("hepp" + i), "hoj"));
			} else {
				command = new Exec(new Identifier(1));
			}
			if (abandon) {
				sender.abandon(command);
			} else {
				sender.send(command);
				middle++;
				if (middle > middlesize) {
					long diff = System.currentTimeMillis() - mtime;
					System.out.println("\n@" + i + ", last " + middlesize + ": " + diff + "ms (" + (diff / middlesize) + "ms/op)");
					middle = 1;
					mtime = System.currentTimeMillis();
				}
			}
			System.out.print(".");
			if (rate != 0)
				Thread.sleep((int) 1000.0 / rate);
		}
		if (abandon) {
			for (long i = 0; i < n; i++) {
				sender.receive();
				middle++;
				if (middle > middlesize) {
					long diff = System.currentTimeMillis() - mtime;
					System.out.println("\nLast " + middlesize + ": " + diff + "ms (" + (diff / middlesize) + "ms/op)");
					middle = 1;
					mtime = System.currentTimeMillis();
				}
				System.out.print(",");
			}
		}
		long t2 = System.currentTimeMillis();
		System.out.println("OK (" + (t2 - t) + "ms, " + ((t2 - t) / n) + "ms/op)");
	}

}
