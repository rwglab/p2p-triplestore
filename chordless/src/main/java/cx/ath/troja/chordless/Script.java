/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless;

import java.io.PrintWriter;
import java.io.Writer;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Script {

	protected ScriptEngine engine;

	private Chord chord;

	private StringBuffer output;

	private PrintWriter writer;

	public Script(Chord c) {
		output = new StringBuffer();
		chord = c;
		engine = new ScriptEngineManager().getEngineByName("js");
		writer = new PrintWriter(new Writer() {
			public void close() {
			}

			public void flush() {
			}

			public void write(char[] chars, int offset, int length) {
				Script.this.write(new String(chars, offset, length));
			}
		});
		if (engine != null) {
			engine.getContext().setWriter(writer);
			engine.getBindings(ScriptContext.ENGINE_SCOPE).put("chord", Script.this.getChord());
			try {
				engine.eval("importPackage(Packages.cx.ath.troja.chordless);");
				engine.eval("importPackage(Packages.cx.ath.troja.nja);");
				engine.eval("importPackage(Packages.cx.ath.troja.chordless.commands);");
				engine.eval("importPackage(Packages.cx.ath.troja.chordless.test);");
				engine.eval("importPackage(Packages.java.util);");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public String eval(String s) {
		if (engine == null) {
			write("No script engine initialized!");
		} else {
			try {
				Object response = engine.eval(s);
				write("" + response);
			} catch (ScriptException e) {
				Throwable t = e;
				while (t != null) {
					t.printStackTrace(writer);
					t = t.getCause();
				}
			}
		}
		String returnValue = output.toString();
		output = new StringBuffer();
		return returnValue;
	}

	protected Chord getChord() {
		return chord;
	}

	protected void write(String s) {
		output.append(s);
	}

}