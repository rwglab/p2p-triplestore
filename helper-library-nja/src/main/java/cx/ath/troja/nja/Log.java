/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Log {

	public static Level ALL = Level.ALL;

	public static Level CONFIG = Level.CONFIG;

	public static Level FINE = Level.FINE;

	public static Level FINER = Level.FINER;

	public static Level FINEST = Level.FINEST;

	public static Level INFO = Level.INFO;

	public static Level OFF = Level.OFF;

	public static Level SEVERE = Level.SEVERE;

	public static Level WARNING = Level.WARNING;

	public static Level ERROR = Level.SEVERE;

	public static Level WARN = Level.WARNING;

	public static Level TRACE = Level.FINEST;

	public static Level DEBUG = Level.FINER;

	private static String currentProperties = null;

	private static String defaultProperties = ("handlers = java.util.logging.ConsoleHandler\n" + ".level = INFO\n"
			+ "java.util.logging.ConsoleHandler.formatter = java.util.logging.SimpleFormatter\n" + "java.util.logging.ConsoleHandler.level = ALL");

	public static void setProperties(File f) {
		try {
			LogManager.getLogManager().readConfiguration(new FileInputStream(f));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void setProperties(String s) {
		try {
			currentProperties = s;
			LogManager.getLogManager().readConfiguration(new ByteArrayInputStream(s.getBytes()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String getProperties() {
		return currentProperties;
	}

	static {
		setProperties(defaultProperties);
	}

	public static void setLevel(Object o, Level l) {
		getLogger(o).setLevel(l);
	}

	public static void setLevel(Level l) {
		Logger.getLogger("").setLevel(l);
	}

	public static Logger getLogger(Object object) {
		return Logger.getLogger(getName(object));
	}

	public static boolean loggable(Object o, Level level) {
		return getLogger(o).isLoggable(level);
	}

	private static String getName(Object o) {
		if (o instanceof String) {
			return (String) o;
		} else if (o instanceof Class) {
			return ((Class) o).getName();
		} else {
			return o.getClass().getName();
		}
	}

	public static void output(Object object, Level level, String message, Throwable throwable) {
		Throwable t = new RuntimeException();
		t.fillInStackTrace();
		StackTraceElement caller = t.getStackTrace()[2];
		String methodString = caller.getMethodName() + ":" + caller.getFileName() + "#" + caller.getLineNumber();
		if (throwable == null) {
			getLogger(object).logp(level, getName(object), methodString, message);
		} else {
			getLogger(object).logp(level, getName(object), methodString, message, throwable);
		}
	}

	public static void trace(Object o, String s, Throwable t) {
		output(o, TRACE, s, t);
	}

	public static void debug(Object o, String s, Throwable t) {
		output(o, DEBUG, s, t);
	}

	public static void info(Object o, String s, Throwable t) {
		output(o, INFO, s, t);
	}

	public static void warn(Object o, String s, Throwable t) {
		output(o, WARN, s, t);
	}

	public static void error(Object o, String s, Throwable t) {
		output(o, ERROR, s, t);
	}

	public static void trace(Object o, String s) {
		output(o, TRACE, s, null);
	}

	public static void debug(Object o, String s) {
		output(o, DEBUG, s, null);
	}

	public static void info(Object o, String s) {
		output(o, INFO, s, null);
	}

	public static void warn(Object o, String s) {
		output(o, WARN, s, null);
	}

	public static void error(Object o, String s) {
		output(o, ERROR, s, null);
	}

}