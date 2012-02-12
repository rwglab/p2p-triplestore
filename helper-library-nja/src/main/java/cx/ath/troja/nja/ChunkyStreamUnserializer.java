/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;


/**
 * A class that is used to unserialize object streams where the objects come in chunks.
 */
public abstract class ChunkyStreamUnserializer extends ChunkyStreamReceiver {

	/**
	 * Called whenever an entire Object has been decoded.
	 * 
	 * @param o
	 *            the object from the stream
	 */
	public abstract void handleObject(Object o);

	@Override
	public void handleChunk(byte[] bytes) {
		handleObject(Cerealizer.unpack(bytes));
	}

	public static byte[] pack(Object s) {
		byte[] data = Cerealizer.pack(s);
		return pack(data);
	}

}