/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tiny utility to parse command line arguments of name=value format.
 */
public class RuntimeArguments extends HashMap {

	private static final Pattern switchPattern = Pattern.compile("(.*)=(.*)");

	private Map<String, String> backend = new HashMap<String, String>();

	/**
	 * Create an instance with no arguments.
	 */
	public RuntimeArguments() {
	}

	/**
	 * Create an instance.
	 * 
	 * @param arguments
	 *            the runtime arguments from a main method (ie String[] argv)
	 */
	public RuntimeArguments(String... arguments) {
		for (int i = 0; i < arguments.length; i++) {
			append(arguments[i], true);
		}
	}

	/**
	 * Add an argument after creation.
	 * 
	 * @param s
	 *            the new argument, formatted as KEY=VALUE
	 * @param overwrite
	 *            whether existing keys should be overwritten
	 */
	public void append(String s, boolean overwrite) {
		Matcher matcher = switchPattern.matcher(s);
		String key = null;
		String value = null;
		if (matcher.matches()) {
			key = matcher.group(1);
			value = matcher.group(2);
		} else {
			key = s;
		}
		if (!backend.containsKey(key) || overwrite) {
			backend.put(key, value);
		}
	}

	/**
	 * Return all arguments.
	 * 
	 * @return a map of arguments
	 */
	public Map<String, String> all() {
		return Collections.unmodifiableMap(backend);
	}

	/**
	 * Return an argument with a given name.
	 * 
	 * @param key
	 *            the name of the argument
	 * @return the value of the argument
	 */
	public String get(String key) {
		return backend.get(key);
	}

	/**
	 * Check if an argument exists.
	 * 
	 * @param key
	 *            the name of the argument
	 * @return whether the argument exists
	 */
	public boolean has(String key) {
		return backend.containsKey(key);
	}

	/**
	 * Return an argument with a given name, and raise an exception with a given message if the argument is missing.
	 */
	public String mustGet(String key, String message) {
		if (backend.containsKey(key)) {
			return backend.get(key);
		} else {
			throw new RuntimeException(message);
		}
	}

	/**
	 * Return an argument or a default value.
	 * 
	 * @param key
	 *            the name of the argument
	 * @param def
	 *            the default value to use if it does not exist
	 * @return the value of the argument, or the default value
	 */
	public String def(String key, String def) {
		String returnValue = backend.get(key);
		if (returnValue == null) {
			returnValue = def;
		}
		return returnValue;
	}

}