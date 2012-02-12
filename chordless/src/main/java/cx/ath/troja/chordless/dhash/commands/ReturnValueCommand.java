/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import cx.ath.troja.chordless.ServerInfo;

public abstract class ReturnValueCommand<T> extends DHashCommand {

	protected T returnValue = null;

	public ReturnValueCommand(ServerInfo i) {
		super(i);
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " returnValue='" + returnValue + "' uuid='" + uuid + "'>";
	}

	public void setReturnValue(T t) {
		returnValue = t;
	}

	public T getReturnValue(ClassLoader cl) {
		return getReturnValue();
	}

	public T getReturnValue() {
		return returnValue;
	}

}
