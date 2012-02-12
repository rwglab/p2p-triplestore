/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Receiver;
import cx.ath.troja.chordless.dhash.RemoteDhasher;
import cx.ath.troja.nja.Identifier;

public class RemoteDHashCommand<T> extends Command implements RemoteDhasher.RemoteDhasherCommand {

	private ReturnValueCommand<T> nestedCommand;

	private Identifier identifier;

	@SuppressWarnings("unchecked")
	public RemoteDHashCommand(Identifier i, DHashCommand c) {
		super(null);
		identifier = i;
		nestedCommand = (ReturnValueCommand<T>) c;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " uuid='" + uuid + "' nestedCommand='" + nestedCommand + "'>";
	}

	public ReturnValueCommand<T> getNestedCommand() {
		return nestedCommand;
	}

	private void setNestedCommand(ReturnValueCommand<T> c) {
		nestedCommand = c;
	}

	@Override
	public void run(RemoteDhasher d) {
		d.deliver(this);
	}

	@Override
	public void execute(final Chord chord, final Sender sender) {
		DHash dhash = (DHash) chord;
		nestedCommand.setCaller(chord.getServerInfo());
		dhash.registerAndSendToSuccessor(identifier, nestedCommand, new Receiver<ReturnValueCommand<T>>() {
			public long getTimeout() {
				return DEFAULT_TIMEOUT;
			}

			public void receive(ReturnValueCommand<T> command) {
				RemoteDHashCommand.this.setNestedCommand(command);
				sender.send(RemoteDHashCommand.this);
			}
		});
	}

}
