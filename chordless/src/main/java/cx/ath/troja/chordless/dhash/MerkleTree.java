/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import static cx.ath.troja.nja.Log.warn;

import java.util.Map;
import java.util.SortedSet;

import cx.ath.troja.nja.Identifier;

public abstract class MerkleTree {

	public int count(Identifier min, Identifier max) {
		if (min.compareTo(max) > 0) {
			return _count(min, Identifier.getMAX_IDENTIFIER()) + _count(new Identifier(0), max);
		} else {
			return _count(min, max);
		}
	}

	/**
	 * Get the number of entries within a given continous range.
	 * 
	 * @param min
	 *            all entries with identifier equal to or bigger than this will be counted
	 * @param max
	 *            all entries with identifier equal to or less than this will be counted
	 * @return the number of entries within the range
	 */
	protected abstract int _count(Identifier min, Identifier max);

	/**
	 * Get identifiers and timestamps in the storage between two points.
	 * 
	 * The returned entries will not have their values set.
	 * 
	 * The two points may overlap 0, ie from may be greater than toAndIncluding, in which case all entries bigger than
	 * from OR smaller than or equal to toAndIncluding shall be returned.
	 * 
	 * @param from
	 *            any identifiers above this will be retrieved
	 * @param toAndIncluding
	 *            any identifiers with equal to or below this will be retrieved
	 * @return all entries between the two points, without content set
	 */
	protected abstract Map<Identifier, Entry> getEmpty(Identifier from, Identifier toAndIncluding);

	/**
	 * Update a node in the storage.
	 * 
	 * The storage only needs to update and save the attributes created by the standard constructor.
	 * 
	 * @param node
	 *            the node to update
	 */
	protected abstract void updateMerkleNode(MerkleNode node);

	/**
	 * Insert a node into the storage.
	 * 
	 * The storage only needs to save the attributes created by the standard constructor.
	 * 
	 * @param node
	 *            the node to insert.
	 */
	protected abstract void insertMerkleNode(MerkleNode node);

	/**
	 * Get a merkle node with given id.
	 * 
	 * The returned node should only have the attributes created by the standard constructor set.
	 * 
	 * @param id
	 *            the id of the wanted node
	 * @return the merkle node that has the given id
	 */
	protected abstract MerkleNode _getMerkleNode(MerkleNode.ID id);

	/**
	 * Get all children of a given merkle node.
	 * 
	 * The returned nodes should only have the attributes created by the standard constructor set.
	 * 
	 * @param parentId
	 *            the id of the parent of the nodes we want
	 * @return all merkle nodes having the given parent
	 */
	protected abstract SortedSet<MerkleNode> _getMerkleChildren(MerkleNode.ID parentId);

	public SortedSet<MerkleNode> getMerkleChildren(MerkleNode.ID parentId) {
		SortedSet<MerkleNode> returnValue = _getMerkleChildren(parentId);
		for (MerkleNode node : returnValue) {
			if (node.isLeaf()) {
				node.setLeafs(getEmpty(node.id.min.previous(), node.id.max));
			}
		}
		return returnValue;
	}

	/**
	 * Delete all children of a given merkle node.
	 * 
	 * @param parentId
	 *            the id of the parent of the nodes we want to delete
	 */
	protected abstract void deleteMerkleChildren(MerkleNode.ID parentId);

	private void merkleInsert(Identifier i, MerkleNode node) {
		if (node.isLeaf() && node.getLeafs().size() >= MerkleNode.TREE_WIDTH) {
			convertToInternal(node);
		}
		if (!node.isLeaf()) {
			merkleInsert(i, getMerkleNode(node.id.getChild(i), true));
		}
		rehash(node);
		updateMerkleNode(node);
	}

	private void convertToInternal(MerkleNode node) {
		node.setIsLeaf(false);
		for (MerkleNode.ID childID : node.id.getChildren()) {
			createLeafNode(new MerkleNode(childID, true, new Identifier(0)));
		}
	}

	private void merkleRemove(Identifier i, MerkleNode node) {
		if (!node.isLeaf() && count(node.id.min, node.id.max) <= MerkleNode.TREE_WIDTH) {
			convertToLeaf(node);
		}
		if (!node.isLeaf()) {
			merkleRemove(i, getMerkleNode(node.id.getChild(i), true));
		}
		rehash(node);
		updateMerkleNode(node);
	}

	private void convertToLeaf(MerkleNode node) {
		node.setIsLeaf(true);
		deleteMerkleChildren(node.id);
	}

	protected void merkleInsert(Identifier i) {
		merkleInsert(i, getRootNode(true));
	}

	protected void merkleRemove(Identifier i) {
		merkleRemove(i, getRootNode(true));
	}

	public MerkleNode getRootNode() {
		return getRootNode(false);
	}

	private MerkleNode getRootNode(boolean create) {
		return getMerkleNode(new MerkleNode.ID(new Identifier(0), Identifier.getMAX_IDENTIFIER()), create);
	}

	public MerkleNode getMerkleNode(MerkleNode.ID id) {
		return getMerkleNode(id, false);
	}

	private MerkleNode getMerkleNode(MerkleNode.ID id, boolean create) {
		MerkleNode returnValue = _getMerkleNode(id);
		if (returnValue == null) {
			returnValue = new MerkleNode(id, true, Identifier.generate(""));
			if (create) {
				insertMerkleNode(returnValue);
			}
		}
		if (returnValue.isLeaf()) {
			returnValue.setLeafs(getEmpty(id.min.previous(), id.max));
		}
		return returnValue;
	}

	private void rehash(MerkleNode node) {
		if (node.isLeaf()) {
			node.setLeafs(getEmpty(node.id.min.previous(), node.id.max));
		} else {
			node.setHash(_getMerkleChildren(node.id));
		}
	}

	public void createLeafNode(MerkleNode node) {
		rehash(node);
		insertMerkleNode(node);
	}

	public void rebuildMerkleNode(MerkleNode node) {
		if (node.isLeaf() && node.getLeafs().size() > MerkleNode.TREE_WIDTH) {
			convertToInternal(node);
			warn(this, "" + node + " had too many children and was converted to an internal node.");
		}
		if (!node.isLeaf()) {
			if (count(node.id.min, node.id.max) <= MerkleNode.TREE_WIDTH) {
				convertToLeaf(node);
				warn(this, "" + node + " had too few children and was converted to a leaf node.");
			} else {
				for (MerkleNode child : getMerkleChildren(node.id)) {
					rebuildMerkleNode(child);
				}
			}
		}
		Identifier oldHash = node.getHash();
		rehash(node);
		if (!oldHash.equals(node.getHash())) {
			warn(this, "" + node + " had a bad hash value of " + oldHash.toString());
			updateMerkleNode(node);
		}
	}

	public void rebuildMerkleTree() {
		rebuildMerkleNode(getRootNode());
	}

}