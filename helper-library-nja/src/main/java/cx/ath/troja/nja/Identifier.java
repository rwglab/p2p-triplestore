/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Random;

public class Identifier implements Comparable<Identifier>, Serializable {

	private static final long serialVersionUID = 1L;

	private static BigInteger MOD_VALUE;

	private static Identifier MAX_IDENTIFIER;

	private static int MAX_STRING_LENGTH;

	private static int BITS;

	private static InetAddress localhost;

	private static Random random;

	static {
		try {
			byte[] example = Checksum.bytes("test".getBytes());
			MOD_VALUE = BigInteger.valueOf(2).pow(example.length * 8);
			BITS = example.length * 8;
			MAX_STRING_LENGTH = example.length * 2;
			localhost = InetAddress.getLocalHost();
			random = new Random();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static int getBITS() {
		return BITS;
	}

	public static Identifier getMAX_IDENTIFIER() {
		if (MAX_IDENTIFIER == null) {
			MAX_IDENTIFIER = new Identifier(MOD_VALUE.subtract(BigInteger.valueOf(1)));
		}
		return MAX_IDENTIFIER;
	}

	public static BigInteger getMOD_VALUE() {
		return MOD_VALUE;
	}

	public static Identifier generate(Object s) {
		if (s instanceof Identifier) {
			return (Identifier) s;
		} else if (s instanceof String && ((String) s).matches("[0-9a-fA-F]{40,40}")) {
			return new Identifier((String) s);
		} else {
			return new Identifier(Cerealizer.pack(s));
		}
	}

	public static Identifier random() {
		return Identifier.generate("" + localhost + ":" + System.currentTimeMillis() + ":" + random.nextLong());
	}

	private BigInteger value;

	public Identifier(BigInteger b) {
		value = b;
	}

	public Identifier(int i) {
		value = BigInteger.valueOf(i);
	}

	public Identifier(String hexString) {
		this(new BigInteger(hexString, 16));
	}

	public Identifier(byte[] bytes) {
		value = Checksum.sum(bytes);
	}

	public Identifier toFingerIdentifier(int fingerIndex) {
		return new Identifier(value.add(BigInteger.valueOf(2).pow(fingerIndex)).mod(MOD_VALUE));
	}

	public BigInteger getValue() {
		return value;
	}

	public byte[] toByteArray() {
		byte[] valueBytes = value.toByteArray();
		byte[] returnValue = new byte[BITS / 8];
		System.arraycopy(valueBytes, 0, returnValue, returnValue.length - valueBytes.length, valueBytes.length);
		return returnValue;
	}

	public String toShortString() {
		String s = toString();
		return s.substring(0, 4) + ".." + s.substring(s.length() - 4, s.length());
	}

	public String toString() {
		StringBuffer returnValue = new StringBuffer(value.toString(16));
		while (returnValue.length() < MAX_STRING_LENGTH) {
			returnValue.insert(0, "0");
		}
		return returnValue.toString();
	}

	public boolean equals(Object o) {
		if (o instanceof Identifier) {
			return value.equals(((Identifier) o).getValue());
		} else {
			return false;
		}
	}

	public boolean betweenGT_LTE(Identifier from, Identifier to) {
		int cFrom = compareTo(from);
		int cTo = compareTo(to);
		if (cFrom > 0 && cTo <= 0) {
			return true;
		} else {
			int fromCto = from.compareTo(to);
			if (fromCto < 0) {
				return false;
			} else if (fromCto > 0) {
				return (cFrom > 0) || (cTo <= 0);
			} else {
				return true;
			}
		}
	}

	public boolean betweenGT_LT(Identifier from, Identifier to) {
		int cFrom = compareTo(from);
		int cTo = compareTo(to);
		if (cFrom > 0 && cTo < 0) {
			return true;
		} else {
			int fromCto = from.compareTo(to);
			if (fromCto < 0) {
				return false;
			} else if (fromCto > 0) {
				return (cFrom > 0) || (cTo < 0);
			} else {
				return true;
			}
		}
	}

	public boolean betweenGTE_LT(Identifier from, Identifier to) {
		int cFrom = compareTo(from);
		int cTo = compareTo(to);
		if (cFrom >= 0 && cTo < 0) {
			return true;
		} else {
			int fromCto = from.compareTo(to);
			if (fromCto < 0) {
				return false;
			} else if (fromCto > 0) {
				return (cFrom >= 0) || (cTo < 0);
			} else {
				return true;
			}
		}
	}

	public boolean betweenGTE_LTE(Identifier from, Identifier to) {
		int cFrom = compareTo(from);
		int cTo = compareTo(to);
		if (cFrom >= 0 && cTo <= 0) {
			return true;
		} else {
			int fromCto = from.compareTo(to);
			if (fromCto < 0) {
				return false;
			} else if (fromCto > 0) {
				return (cFrom >= 0) || (cTo <= 0);
			} else {
				return true;
			}
		}
	}

	public Identifier distanceTo(Identifier other) {
		int c = compareTo(other);
		if (c < 0) {
			return new Identifier(other.getValue().subtract(getValue()));
		} else if (c > 0) {
			return new Identifier(other.getValue().add(MOD_VALUE.subtract(getValue())));
		} else {
			return new Identifier(MOD_VALUE);
		}
	}

	public Identifier min(Identifier other) {
		if (compareTo(other) > 0) {
			return other;
		} else {
			return this;
		}
	}

	public Identifier max(Identifier other) {
		if (compareTo(other) > 0) {
			return this;
		} else {
			return other;
		}
	}

	public Identifier previous() {
		if (equals(new Identifier(0))) {
			return getMAX_IDENTIFIER();
		} else {
			return new Identifier(value.subtract(BigInteger.valueOf(1)));
		}
	}

	public Identifier next() {
		if (equals(getMAX_IDENTIFIER())) {
			return new Identifier(0);
		} else {
			return new Identifier(value.add(BigInteger.valueOf(1)));
		}
	}

	public int hashCode() {
		return value.hashCode();
	}

	public int compareTo(Identifier other) {
		return value.compareTo(other.getValue());
	}

}
