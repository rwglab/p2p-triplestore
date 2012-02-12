/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.TRACE;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.trace;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * A class that makes it easy to unserialize objects with a custom classloader.
 */
public class DynamicObjectInputStream extends ObjectInputStream {

	private ClassLoader loader;

	/**
	 * Create a new dynamic ObjectInputStream.
	 * 
	 * @param i
	 *            the stream to read from
	 * @param l
	 *            the class loader to use when resolving classes
	 */
	public DynamicObjectInputStream(InputStream i, ClassLoader l) throws IOException {
		super(i);
		loader = l;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Class resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
		if (loggable(this, TRACE)) {
			trace(this, "Asked to resolve " + desc);
		}
		return Class.class.forName(desc.getName(), false, loader);
	}

}