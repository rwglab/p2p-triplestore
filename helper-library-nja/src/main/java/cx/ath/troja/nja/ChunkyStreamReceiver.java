/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;

/**
 * A class that is used to unserialize object streams where the objects come in chunks.
 */
public abstract class ChunkyStreamReceiver {

	private ByteArrayOutputStream headerBuffer = new ByteArrayOutputStream();

	private ByteArrayOutputStream dataBuffer = new ByteArrayOutputStream();

	private int expectedSize = -1;

	/**
	 * Called whenever an entire chunk has been received.
	 * 
	 * @param bytes
	 *            the chunk from the stream
	 */
	public abstract void handleChunk(byte[] bytes);

	public static final int HEADER_SIZE = 4;

	public static byte[] pack(byte[] data) {
		byte[] returnValue = new byte[data.length + HEADER_SIZE];
		System.arraycopy(data, 0, returnValue, HEADER_SIZE, data.length);
		byte[] lengthPart = BigInteger.valueOf(data.length).toByteArray();
		System.arraycopy(lengthPart, 0, returnValue, HEADER_SIZE - lengthPart.length, lengthPart.length);
		return returnValue;
	}

	public void handleBytes(byte[] b) {
		handleBytes(b, 0, b.length);
	}

	public void handleBytes(byte[] incomingBytes, int incomingOffset, int incomingLength) {
		int offset = incomingOffset;

		try {
			while (offset < incomingLength) {
				if (headerBuffer.size() < HEADER_SIZE) {
					int toWrite = Math.min(HEADER_SIZE - headerBuffer.size(), incomingLength - offset);
					headerBuffer.write(incomingBytes, offset, toWrite);
					offset += toWrite;
				}
				if (headerBuffer.size() == HEADER_SIZE) {
					if (expectedSize == -1) {
						expectedSize = new BigInteger(headerBuffer.toByteArray()).intValue();
						if (expectedSize <= 0) {
							throw new RuntimeException("Illegal header - size " + expectedSize + " is not valid!");
						}
					}
					if (dataBuffer.size() < expectedSize) {
						int toWrite = Math.min(expectedSize - dataBuffer.size(), incomingLength - offset);
						dataBuffer.write(incomingBytes, offset, toWrite);
						offset += toWrite;
					}
					if (dataBuffer.size() == expectedSize) {
						handleChunk(dataBuffer.toByteArray());
						headerBuffer = new ByteArrayOutputStream();
						dataBuffer = new ByteArrayOutputStream();
						expectedSize = -1;
					} else if (dataBuffer.size() > expectedSize) {
						throw new RuntimeException("How the hell did the header buffer become bigger than expected (" + expectedSize + ")?");
					}
				} else if (headerBuffer.size() > HEADER_SIZE) {
					throw new RuntimeException("How the hell did the header buffer become bigger than " + HEADER_SIZE + "?");
				}
			}
		} catch (Throwable t) {
			throw new RuntimeException("Problems unpacking data", t);
		}
	}

}