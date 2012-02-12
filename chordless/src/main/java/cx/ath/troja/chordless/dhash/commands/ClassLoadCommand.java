/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.error;
import static cx.ath.troja.nja.Log.loggable;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.RemoteDhasher;
import cx.ath.troja.nja.Checksum;
import cx.ath.troja.nja.Identifier;

public class ClassLoadCommand extends ReturnValueCommand<byte[]> implements RemoteDhasher.RemoteDhasherCommand {

	private String className;

	private ServerInfo destination;

	public ClassLoadCommand(ServerInfo c, ServerInfo d, String n) {
		super(c);
		destination = d;
		className = n;
	}

	@Override
	protected Identifier getRegardingIdentifier() {
		return destination.getIdentifier();
	}

	public String dataDigest() {
		return Checksum.hex(returnValue);
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " caller='" + caller + "' className='" + className + "' done='" + done + "' dataDigest='"
				+ (returnValue == null ? "null" : dataDigest()) + "' uuid='" + uuid + "'>";
	}

	@Override
	public int getPriority() {
		return -40;
	}

	private String classNameToResource(String className) {
		return "/" + className.replaceAll("\\.", "/") + ".class";
	}

	@Override
	protected void executeHome(DHash dhash) {
		if (loggable(this, DEBUG))
			debug(this, "" + this + " returning with the goods");
		dhash.deliver(this);
	}

	public String getClassName() {
		return className;
	}

	public void run(RemoteDhasher d) {
		done = true;
		loadClass();
		d.send(getCaller().getAddress(), this);
	}

	public void loadClass() {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		InputStream input = this.getClass().getResourceAsStream(classNameToResource(className));
		if (input == null) {
			error(this, "Unable to find class " + className);
		}
		byte[] bytes = new byte[1024];
		int n;
		try {
			while ((n = input.read(bytes)) != -1) {
				byteOut.write(bytes, 0, n);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		returnValue = byteOut.toByteArray();
	}

	@Override
	protected void executeAway(DHash dhash) {
		loadClass();
		returnHome(dhash);
	}

}
