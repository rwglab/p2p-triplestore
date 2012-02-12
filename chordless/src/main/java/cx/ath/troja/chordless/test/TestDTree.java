///*
// * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
// * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
// * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// * General Public License for more details. You should have received a copy of the GNU General Public License along with
// * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
// */
//
//package cx.ath.troja.chordless.test;
//
//import java.util.HashSet;
//import java.util.Set;
//
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.structures.DTree;
//import cx.ath.troja.chordless.dhash.transactions.Transaction;
//import cx.ath.troja.nja.Identifier;
//
//public class TestDTree extends TestDHash {
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		test(arguments);
//		teardown();
//	}
//
//	public static void testDTreeElementRemoval(Dhasher d) throws Exception {
//		System.out.print("\tTesting element removal");
//		Set<String> comp = new HashSet<String>();
//		DTree<String> tree = new DTree<String>();
//		d.put(tree).get();
//		dot();
//		Delay<Identifier> delay1 = d.exec(tree.getIdentifier(), "append", "element1");
//		comp.add("element1");
//		delay1.get();
//		dot();
//		Delay<Identifier> delay2 = d.exec(tree.getIdentifier(), "append", "element2");
//		comp.add("element2");
//		delay2.get();
//		dot();
//		Delay<Identifier> delay3 = d.exec(tree.getIdentifier(), "append", "element3");
//		comp.add("element3");
//		delay3.get();
//		dot();
//		if (!comp.equals(new HashSet<Object>(getContent(d, tree))))
//			throw new RuntimeException("not right content, wanted " + comp + " but got " + getContent(d, tree));
//		dot();
//		d.exec(delay2.get(), "remove").get();
//		dot();
//		comp.remove("element2");
//		if (!comp.equals(new HashSet<Object>(getContent(d, tree))))
//			throw new RuntimeException("not right content");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void testDTreeTransactions(Dhasher d) throws Exception {
//		System.out.print("\tTesting DTree transaction semantics");
//		Set<String> comp = new HashSet<String>();
//		DTree<String> tree = new DTree<String>();
//		d.put(tree).get();
//		dot();
//		Delay<Identifier> delay1 = d.exec(tree.getIdentifier(), "add", "element1");
//		comp.add("element1");
//		delay1.get();
//		dot();
//		Delay<Identifier> delay2 = d.exec(tree.getIdentifier(), "add", "element2");
//		comp.add("element2");
//		delay2.get();
//		dot();
//		Delay<Identifier> delay3 = d.exec(tree.getIdentifier(), "add", "element3");
//		comp.add("element3");
//		delay3.get();
//		dot();
//		if (!comp.equals(new HashSet<Object>(getContent(d, tree))))
//			throw new RuntimeException("not right content");
//		dot();
//		Transaction t1 = d.transaction();
//		Transaction t2 = d.transaction();
//		t1.exec(tree.getIdentifier(), "add", "trans1").get();
//		dot();
//		t2.exec(tree.getIdentifier(), "add", "trans2").get();
//		dot();
//		t1.commit();
//		dot();
//		t2.commit();
//		dot();
//		comp.add("trans1");
//		comp.add("trans2");
//		if (!comp.equals(new HashSet<Object>(getContent(d, tree))))
//			throw new RuntimeException("not right content");
//		dot();
//		Transaction t3 = d.transaction();
//		t1 = t3.transaction();
//		t2 = t3.transaction();
//		t1.exec(tree.getIdentifier(), "add", "trans11").get();
//		dot();
//		t2.exec(tree.getIdentifier(), "add", "trans22").get();
//		dot();
//		t1.commit();
//		dot();
//		t2.commit();
//		dot();
//		t3.commit();
//		dot();
//		comp.add("trans11");
//		comp.add("trans22");
//		if (!comp.equals(new HashSet<Object>(getContent(d, tree))))
//			throw new RuntimeException("not right content");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		System.out.println("Testing DTree");
//
//		Dhasher d = new Dhasher(dhashes[0]);
//
//		testDTreeElementRemoval(d);
//		testDTreeTransactions(d);
//
//	}
//
//}
