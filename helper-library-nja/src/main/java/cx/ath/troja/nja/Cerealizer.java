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
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A class that wraps the chunky serializing in java.
 */
public class Cerealizer {

	public static class ClassNotFoundException extends RuntimeException {
		private String className;

		public ClassNotFoundException(Throwable t, String c) {
			super(c, t);
			className = c;
		}

		public ClassNotFoundException(String s) {
			super(s);
		}

		public String getClassName() {
			return className;
		}
	}

	/**
	 * Serialize something.
	 * 
	 * @param s
	 *            somethign to serialize
	 * @return the bytes it became
	 */
	public static byte[] pack(Object o) {
		try {
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
			objectOut.writeObject(o);
			objectOut.close();
			byteOut.close();
			return byteOut.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Unserialize some bytes.
	 * 
	 * @param c
	 *            what to cast the result to
	 * @param bytes
	 *            what to unserialize
	 * @return whatever the bytes was unserialized to
	 */
	@SuppressWarnings("unchecked")
	public static <T> T unpack(Class<T> c, byte[] bytes) {
		return (T) unpack(bytes);
	}

	/**
	 * Unserialize some bytes using a given classloader.
	 * 
	 * @param bytes
	 *            what to unserialize
	 * @param loader
	 *            the classloader to use
	 * @return whatever the bytes was unserialized to
	 */
	public static Object unpack(byte[] bytes, ClassLoader loader) {
		try {
			ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
			DynamicObjectInputStream dynamicObjectIn = new DynamicObjectInputStream(byteIn, loader);
			return dynamicObjectIn.readObject();
		} catch (java.lang.ClassNotFoundException e) {
			throw new Cerealizer.ClassNotFoundException(e, e.getMessage());
		} catch (NoClassDefFoundError e) {
			throw new Cerealizer.ClassNotFoundException(e, e.getCause().getMessage());
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Unserialize some bytes.
	 * 
	 * @param bytes
	 *            what to unserialize
	 * @return whatever the bytes was unserialized to
	 */
	public static Object unpack(byte[] bytes) {
		try {
			ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
			ObjectInputStream objectIn = new ObjectInputStream(byteIn);
			return objectIn.readObject();
		} catch (java.lang.ClassNotFoundException e) {
			throw new Cerealizer.ClassNotFoundException(e, e.getMessage());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}