/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import java.io.Serializable;

import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Checksum;
import cx.ath.troja.nja.Identifier;

/**
 * A few words on the different versioning schemes used for Entries:
 * 
 * Non-commutative changes (due to put, exec, del etc) to an Entry will increment version, iteration and commutation,
 * commutative changes (due to commute) will increment iteration and commutation, metadata changes (due to lock/unlock,
 * mostly) will increment only iteration,
 * 
 * This is because synchronization and cleanup needs to compare Entries and see even metadata changes, while
 * transactions need to know whether commutative and/or non-commutative have happened.
 */
public class Entry implements Serializable, Comparable<Entry> {

	/**
	 * The number of milliseconds that we keep null entries in the storage.
	 */
	public static int NULL_ENTRY_LIFETIME = (1000 * 60 * 60 * 24);

	private Identifier key;

	private byte[] bytes;

	private long timestamp;

	private long version;

	private long iteration;

	private long commutation;

	private String valueClassName;

	private Identifier locker;

	public Entry(Persistent p) {
		this(p.getIdentifier(), p);
	}

	public Entry(Identifier k, Object v) {
		if (k == null) {
			throw new IllegalArgumentException("" + this.getClass().getName() + " doesn't allow null key (" + k + ")!");
		}
		if (v instanceof Persistent) {
			Persistent p = (Persistent) v;
			if (!p.getIdentifier().equals(k)) {
				throw new IllegalArgumentException("You can't provide a Persistent object (" + v + ") identified by " + p.getIdentifier()
						+ " with a different Identifier (" + k + ")!");
			}
		}
		if (v == null) {
			valueClassName = "null";
		} else {
			valueClassName = v.getClass().getName();
		}
		key = k;
		bytes = Cerealizer.pack(v);
		timestamp = System.currentTimeMillis();
		version = 1;
		iteration = 1;
		commutation = 1;
		locker = null;
	}

	public Identifier getLocker() {
		return locker;
	}

	public boolean fresh() {
		return !valueClassName.equals("null") || (System.currentTimeMillis() - timestamp) < NULL_ENTRY_LIFETIME;
	}

	public String valueDigest() {
		if (bytes == null) {
			return "null";
		} else {
			return Checksum.hex(bytes);
		}
	}

	public String toString() {
		return "<" + this.getClass().getName() + " key=" + key + " timestamp=" + timestamp + " version=" + version + " iteration=" + iteration
				+ " commutation=" + commutation + " valueClassName=" + valueClassName + ">";
	}

	public Entry(Identifier k, Object v, long t, long ver, long com, long iter, String className, Identifier i) {
		key = k;
		bytes = Cerealizer.pack(v);
		timestamp = t;
		version = ver;
		iteration = iter;
		commutation = com;
		valueClassName = className;
		locker = i;
	}

	public Entry(String k, byte[] v, long t, long ver, long com, long iter, String className, String l) {
		key = new Identifier(k);
		bytes = v;
		timestamp = t;
		version = ver;
		iteration = iter;
		commutation = com;
		valueClassName = className;
		if (l == null) {
			locker = null;
		} else {
			locker = new Identifier(l);
		}
	}

	public void clearBytes() {
		bytes = null;
	}

	public int hashCode() {
		return key.hashCode();
	}

	public String getValueClassName() {
		return valueClassName;
	}

	public String getIdentifierString() {
		return key.toString();
	}

	public byte[] getBytes() {
		return bytes;
	}

	public long getVersion() {
		return version;
	}

	public long getCommutation() {
		return commutation;
	}

	public long getIteration() {
		return iteration;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void bumpVersion(Entry old) {
		iteration = old.getIteration() + 1;
		commutation = old.getCommutation() + 1;
		version = old.getVersion() + 1;
	}

	public void bumpCommutation(Entry old) {
		iteration = old.getIteration() + 1;
		commutation = old.getCommutation() + 1;
		version = old.getVersion();
	}

	public Identifier getIdentifier() {
		return key;
	}

	public Object getValue(ClassLoader cl) {
		return Cerealizer.unpack(bytes, cl);
	}

	public Object getValue() {
		return getValue(getClass().getClassLoader());
	}

	public boolean equals(Object o) {
		if (o instanceof Entry) {
			Entry other = (Entry) o;
			return other.getIdentifier().equals(getIdentifier());
		} else {
			return false;
		}
	}

	public int compareTo(Entry other) {
		return key.compareTo(other.getIdentifier());
	}

}