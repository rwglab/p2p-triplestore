/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import javax.script.ScriptContext;
import javax.script.ScriptException;

import cx.ath.troja.chordless.Script;

public class DHashScript extends Script {

	public DHashScript(DHash dhash) {
		super(dhash);
		if (engine != null) {
			engine.getBindings(ScriptContext.ENGINE_SCOPE).put("dhash", DHashScript.this.getChord());
			engine.getBindings(ScriptContext.ENGINE_SCOPE).put("dhasher", new Dhasher((DHash) DHashScript.this.getChord()));
			try {
				engine.eval("importPackage(Packages.cx.ath.troja.chordless.dhash);");
				engine.eval("importPackage(Packages.cx.ath.troja.chordless.dhash.commands);");
				engine.eval("importPackage(Packages.cx.ath.troja.chordless.dhash.storage);");
				engine.eval("importPackage(Packages.cx.ath.troja.chordless.dhash.structures);");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		}
	}

}