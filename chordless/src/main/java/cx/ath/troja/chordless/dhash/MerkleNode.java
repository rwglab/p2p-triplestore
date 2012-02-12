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
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cx.ath.troja.nja.Identifier;

public class MerkleNode implements Serializable, Comparable {

	private static final long serialVersionUID = 1L;

	public static int TREE_BITS = 5;

	public static int TREE_WIDTH = (int) Math.pow(2, TREE_BITS);

	public static int TREE_DEPTH = Identifier.getBITS() / TREE_BITS;

	public static class ID implements Serializable, Comparable<ID> {

		public final Identifier min;

		public final Identifier max;

		private int level;

		private BigInteger blankMask;

		private ID parent;

		public ID(Identifier a, Identifier b) {
			min = a;
			max = b;
			level = -1;
			blankMask = null;
			parent = null;
		}

		public int compareTo(ID o) {
			int minCmp = min.compareTo(o.getMin());
			if (minCmp != 0) {
				return minCmp;
			} else {
				return max.compareTo(o.getMax());
			}
		}

		public Identifier getMin() {
			return min;
		}

		public Identifier getMax() {
			return max;
		}

		public String toString() {
			return min.toString() + ":" + max.toString();
		}

		public byte[] toByteArray() {
			byte[] minAry = min.toByteArray();
			byte[] maxAry = max.toByteArray();
			byte[] returnValue = new byte[minAry.length + maxAry.length];
			System.arraycopy(minAry, 0, returnValue, 0, minAry.length);
			System.arraycopy(maxAry, 0, returnValue, minAry.length, maxAry.length);
			return returnValue;
		}

		public boolean equals(Object o) {
			if (o instanceof ID) {
				ID otherID = (ID) o;
				return otherID.min.equals(min) && otherID.max.equals(max);
			} else {
				return false;
			}
		}

		public int getLevel() {
			if (level == -1) {
				BigInteger mask = Identifier.getMOD_VALUE().subtract(BigInteger.valueOf(1));
				for (int i = 0; i < TREE_DEPTH; i++) {
					mask = mask.shiftRight(TREE_BITS);
					if (!min.getValue().andNot(mask).equals(max.getValue().andNot(mask))) {
						level = i;
						break;
					}
				}
				if (level == -1) {
					return TREE_DEPTH - 1;
				}
			}
			return level;
		}

		private BigInteger getBlankMask() {
			if (blankMask == null) {
				blankMask = BigInteger.valueOf(TREE_WIDTH - 1).shiftLeft(Identifier.getBITS() - TREE_BITS - (TREE_BITS * getLevel()));
			}
			return blankMask;
		}

		public List<ID> getChildren() {
			List<ID> returnValue = new LinkedList<ID>();
			BigInteger blankMask = getBlankMask();
			for (int i = 0; i < TREE_WIDTH; i++) {
				BigInteger childMask = BigInteger.valueOf(i).shiftLeft(Identifier.getBITS() - TREE_BITS - (TREE_BITS * getLevel()));
				returnValue.add(new ID(new Identifier(min.getValue().andNot(blankMask).or(childMask)), new Identifier(max.getValue()
						.andNot(blankMask).or(childMask))));
			}
			return returnValue;
		}

		public ID getChild(Identifier identifier) {
			BigInteger blankMask = getBlankMask();
			BigInteger childMask = identifier.getValue().and(blankMask);
			return new ID(new Identifier(min.getValue().andNot(blankMask).or(childMask)), new Identifier(max.getValue().andNot(blankMask)
					.or(childMask)));
		}

		public ID getParent() {
			if (parent == null) {
				BigInteger parentMask = getBlankMask().shiftLeft(TREE_BITS).and(Identifier.getMAX_IDENTIFIER().getValue());
				parent = new ID(new Identifier(min.getValue().andNot(parentMask)), new Identifier(max.getValue().or(parentMask)));
				if (parent.equals(this)) {
					parent = null;
				}
			}
			return parent;
		}
	}

	public final ID id;

	private Identifier hash;

	private boolean isLeaf;

	private Map<Identifier, Entry> leafs = new HashMap<Identifier, Entry>();

	public MerkleNode(ID i, boolean l, Identifier h) {
		id = i;
		hash = h;
		isLeaf = l;
	}

	public String toString() {
		return "<" + getClass().getName() + " ID='" + id + "' hash='" + (hash == null ? "null" : hash.toString()) + "' isLeaf='" + isLeaf
				+ "' no leafs='" + leafs.size() + "'>";
	}

	public int compareTo(Object o) {
		if (o instanceof MerkleNode) {
			return id.compareTo(((MerkleNode) o).id);
		} else {
			return 0;
		}
	}

	public void setHash(Collection<MerkleNode> c) {
		StringBuffer hashSource = new StringBuffer();
		for (MerkleNode node : c) {
			hashSource.append(node.getHash().toString());
		}
		hash = Identifier.generate(hashSource.toString());
	}

	public void setIsLeaf(boolean b) {
		isLeaf = b;
	}

	public Map<Identifier, Entry> getLeafs() {
		return leafs;
	}

	public void setLeafs(Map<Identifier, Entry> c) {
		StringBuffer hashSource = new StringBuffer();
		for (Entry entry : c.values()) {
			hashSource.append(entry.getIdentifier().toString() + entry.getIteration());
		}
		hash = Identifier.generate(hashSource.toString());
		leafs = c;
	}

	public boolean isLeaf() {
		return isLeaf;
	}

	public Identifier getHash() {
		return hash;
	}

}