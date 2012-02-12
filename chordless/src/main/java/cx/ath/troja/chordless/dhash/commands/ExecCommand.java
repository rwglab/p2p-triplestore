/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import java.util.Arrays;
import java.util.Date;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.ExecDhasher;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.FriendlyTask;
import cx.ath.troja.nja.Identifier;

public class ExecCommand extends ExecBase {

	protected String methodName;

	protected byte[] serializedArguments;

	protected String argumentsToString;

	public ExecCommand(ServerInfo caller, ServerInfo classLoaderHost, Identifier identifier, String methodName, Object... arguments) {
		super(caller, classLoaderHost, identifier);
		this.methodName = methodName;
		this.serializedArguments = Cerealizer.pack(arguments);
		this.argumentsToString = Arrays.asList(arguments).toString();
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " identifier='" + identifier + "' methodName='" + methodName + "' arguments='" + argumentsToString
				+ "' caller='" + caller + "' classLoaderHost='" + classLoaderHost + "' returnValue='" + returnValue + "' done='" + done + "' uuid='"
				+ uuid + "'>";
	}

	public String getMethodName() {
		return methodName;
	}

	public Object[] getArguments(ClassLoader classLoader) {
		return (Object[]) Cerealizer.unpack(serializedArguments, classLoader);
	}

	public void done(final DHash dhash, Object object, final Object returnValue, boolean tainted, ExecDhasher dhasher) {
		super.done(dhasher, object, tainted);
		ExecCommand.this.getExecutor(dhash).execute(new FriendlyTask("" + this + ".done(...) running since " + new Date()) {
			public String getDescription() {
				return ExecCommand.this.getClass().getName() + ".run";
			}

			public int getPriority() {
				return ExecCommand.this.getPriority();
			}

			public void subrun() {
				ExecCommand.this.returnValue = returnValue;
				ExecCommand.this.returnHome(dhash);
			}
		});
	}

	@Override
	protected void executeAway(final DHash dhash) {
		dhash.getStorage().exec(dhash, this);
	}

}
