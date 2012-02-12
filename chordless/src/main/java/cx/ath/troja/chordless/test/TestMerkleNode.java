///*
// * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
// * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
// * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// * General Public License for more details. You should have received a copy of the GNU General Public License along with
// * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
// */
//
//package cx.ath.troja.chordless.test;
//
//import java.util.Collection;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//
//import cx.ath.troja.chordless.dhash.DHash;
//import cx.ath.troja.chordless.dhash.Entry;
//import cx.ath.troja.chordless.dhash.MerkleNode;
//import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
//import cx.ath.troja.nja.Identifier;
//import cx.ath.troja.nja.JDBC;
//import cx.ath.troja.nja.ThreadState;
//
//public class TestMerkleNode extends TestAll {
//
//	public static void testNodeIds() {
//		System.out.print("Testing merkle node id integrity");
//		MerkleNode.ID rootID = new MerkleNode.ID(new Identifier(0), Identifier.getMAX_IDENTIFIER());
//		if (rootID.getLevel() != 0)
//			throw new RuntimeException("root id doesnt have level 0, it has level " + rootID.getLevel());
//		if (rootID.getParent() != null)
//			throw new RuntimeException("root id doesnt have null as parent");
//		System.out.print(".");
//		List<MerkleNode.ID> children = rootID.getChildren();
//		for (MerkleNode.ID childID : children) {
//			if (!childID.min.betweenGTE_LTE(rootID.min, rootID.max) || !childID.max.betweenGTE_LTE(rootID.min, rootID.max))
//				throw new RuntimeException("childID is not within rootID!");
//			if (!childID.getParent().equals(rootID))
//				throw new RuntimeException("childID doesnt have root as parent, it has " + childID.getParent());
//			if (childID.getLevel() != 1)
//				throw new RuntimeException("child doesnt have level 1, it has level " + childID.getLevel());
//			for (MerkleNode.ID otherChildID : children) {
//				if (childID != otherChildID) {
//					if (childID.min.betweenGTE_LTE(otherChildID.min, otherChildID.max)
//							|| childID.min.betweenGTE_LTE(otherChildID.min, otherChildID.max))
//						throw new RuntimeException("" + childID + " overlaps " + otherChildID);
//				}
//			}
//			List<MerkleNode.ID> grandChildren = childID.getChildren();
//			for (MerkleNode.ID grandChildID : grandChildren) {
//				if (!grandChildID.getParent().equals(childID))
//					throw new RuntimeException("grandchild doesnt have child as parent!");
//				if (!grandChildID.min.betweenGTE_LTE(childID.min, childID.max) || !grandChildID.max.betweenGTE_LTE(childID.min, childID.max))
//					throw new RuntimeException("grandchild is not within child!");
//				if (grandChildID.getLevel() != 2)
//					throw new RuntimeException("grandchild id doesnt have level 2, it has level " + grandChildID.getLevel());
//				for (MerkleNode.ID otherChildID : children) {
//					if (childID != otherChildID) {
//						if (grandChildID.min.betweenGTE_LTE(otherChildID.min, otherChildID.max)
//								|| grandChildID.min.betweenGTE_LTE(otherChildID.min, otherChildID.max))
//							throw new RuntimeException("" + grandChildID + " overlaps " + otherChildID);
//					}
//				}
//				for (MerkleNode.ID otherGrandChildID : grandChildren) {
//					if (grandChildID != otherGrandChildID) {
//						if (grandChildID.min.betweenGTE_LTE(otherGrandChildID.min, otherGrandChildID.max)
//								|| grandChildID.min.betweenGTE_LTE(otherGrandChildID.min, otherGrandChildID.max))
//							throw new RuntimeException("" + grandChildID + " overlaps " + otherGrandChildID);
//					}
//				}
//				System.out.print(".");
//			}
//			System.out.print(".");
//		}
//		System.out.println("done!");
//	}
//
//	public static String randString() {
//		return "" + ((long) (Math.random() * Long.MAX_VALUE));
//	}
//
//	public static Collection<Entry> inventStuff() {
//		Collection<Entry> returnValue = new LinkedList<Entry>();
//		for (int i = 0; i < 100; i++) {
//			returnValue.add(new Entry(Identifier.generate(randString()), "plupp"));
//		}
//		return returnValue;
//	}
//
//	public static void testMerkleUpdating() throws Exception {
//		System.out.print("Testing merkle tree integrity in Storage");
//		ThreadState.put(ExecutorService.class, DHash.PERSIST_EXECUTOR);
//		JDBC j1 = new JDBC(TestDHash.driver, TestDHash.url + "j1");
//		JDBC j2 = new JDBC(TestDHash.driver, TestDHash.url + "j2");
//		try {
//			j1.execute("delete from chordless_merkle_nodes");
//		} catch (Exception e) {
//		}
//		try {
//			j2.execute("delete from chordless_merkle_nodes");
//		} catch (Exception e) {
//		}
//		try {
//			j1.execute("delete from chordless_entries");
//		} catch (Exception e) {
//		}
//		try {
//			j2.execute("delete from chordless_entries");
//		} catch (Exception e) {
//		}
//		JDBCStorage s1 = new JDBCStorage(j1);
//		JDBCStorage s2 = new JDBCStorage(j2);
//		Collection<Entry> stuff = inventStuff();
//		for (Entry entry : stuff) {
//			put(s1, entry);
//			put(s2, entry);
//			System.out.print(".");
//		}
//		if (!s1.getRootNode().getHash().equals(s2.getRootNode().getHash()))
//			throw new RuntimeException("root of s1 (" + s1.getRootNode() + ") and root of s2 (" + s2.getRootNode() + ") are not equal after writing!");
//		for (int i = 0; i < 10; i++) {
//			for (Entry entry : stuff) {
//				double rand = Math.random();
//				if (rand > 0.9) {
//					del(s1, entry.getIdentifier());
//					del(s2, entry.getIdentifier());
//				} else if (rand > 0.8) {
//					Entry newEntry = new Entry(entry.getIdentifier(), "knapp");
//					put(s1, newEntry);
//					put(s2, newEntry);
//				}
//				System.out.print(".");
//			}
//		}
//		if (!s1.getRootNode().getHash().equals(s2.getRootNode().getHash()))
//			throw new RuntimeException("root of s1 (" + s1.getRootNode() + ") and root of s2 (" + s2.getRootNode()
//					+ ") are not equal after random modifications!");
//		System.out.println("done!");
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		testMerkleUpdating();
//		testNodeIds();
//	}
//}