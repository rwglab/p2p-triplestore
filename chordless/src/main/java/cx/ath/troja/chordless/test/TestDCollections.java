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
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.structures.DCollection;
//import cx.ath.troja.chordless.dhash.structures.DIndexedList;
//import cx.ath.troja.chordless.dhash.structures.DList;
//import cx.ath.troja.chordless.dhash.structures.DSet;
//import cx.ath.troja.chordless.dhash.structures.DSortedSet;
//import cx.ath.troja.chordless.dhash.structures.DTree;
//import cx.ath.troja.nja.Identifier;
//
//public class TestDCollections extends TestDHash {
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		test(arguments);
//		teardown();
//	}
//
//	@SuppressWarnings("unchecked")
//	public static List sort(Collection c) {
//		ArrayList l = new ArrayList(c);
//		Collections.sort(l);
//		return l;
//	}
//
//	public static void testAddSize(Dhasher d, final DCollection c) throws Exception {
//		System.out.print("\t\tTesting add/contains/inject/size/inject#remove");
//		Set<String> comparison = new HashSet<String>();
//		for (int i = 0; i < 10; i++) {
//			if (!d.exec(c.getIdentifier(), "size").get().equals(new Long(i)))
//				throw new RuntimeException("wrong size");
//			if (!d.exec(c.getIdentifier(), "contains", "element" + i).get().equals(Boolean.FALSE))
//				throw new RuntimeException("contains returns true when it should be false");
//			d.exec(c.getIdentifier(), "add", "element" + i).get();
//			comparison.add("element" + i);
//			Delay<Set<String>> injectDelay = d.exec(c.getIdentifier(), "inject", new HashSet<String>(),
//					new DCollection.DefaultDInject<Set<String>, String>() {
//						public Set<String> inject(Set<String> sum, String s) {
//							sum.add(s);
//							return sum;
//						}
//					});
//			if (!injectDelay.get().equals(comparison))
//				throw new RuntimeException("inject did not collect the right entries! got " + injectDelay.get() + " but wanted " + comparison);
//			if (!d.exec(c.getIdentifier(), "contains", "element" + i).get().equals(Boolean.TRUE))
//				throw new RuntimeException("contains returns false when it should be true");
//			dot();
//		}
//		List<String> content = new ArrayList<String>(comparison);
//		while (content.size() > 0) {
//			final String toRemove = content.get((int) (Math.random() * content.size()));
//			Delay<Object> removeDelay = d.exec(c.getIdentifier(), "each", new DCollection.DefaultDEach<String>() {
//				public void each(String s) {
//					if (s.equals(toRemove)) {
//						remove();
//					}
//				}
//			});
//			removeDelay.get();
//			dot();
//			content.remove(toRemove);
//			comparison.remove(toRemove);
//			Delay<Set<String>> injectDelay = d.exec(c.getIdentifier(), "inject", new HashSet<String>(),
//					new DCollection.DefaultDInject<Set<String>, String>() {
//						public Set<String> inject(Set<String> sum, String s) {
//							sum.add(s);
//							return sum;
//						}
//					});
//			if (!injectDelay.get().equals(comparison))
//				throw new RuntimeException("inject did not collect the right entries! got " + sort(injectDelay.get()) + " but wanted "
//						+ sort(comparison));
//			dot();
//			if (!d.exec(c.getIdentifier(), "size").get().equals(new Long(comparison.size())))
//				throw new RuntimeException("wrong size! got " + d.exec(c.getIdentifier(), "size").get() + " but wanted " + comparison.size());
//			dot();
//		}
//		System.out.println("done!");
//	}
//
//	public static void testIterator(Dhasher d, DCollection c) throws Exception {
//		System.out.print("\t\tTesting iterator");
//		Set<String> wanted = new HashSet<String>();
//		for (int i = 0; i < 10; i++) {
//			wanted.add("blah" + i);
//			dot();
//		}
//		checkContent(d, c, wanted);
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void testContinuedIterator(Dhasher d, DCollection c) throws Exception {
//		System.out.print("\t\tTesting continued iterators");
//		Set<String> wanted = new HashSet<String>();
//		for (int i = 0; i < 10; i++) {
//			wanted.add("blah" + i);
//		}
//		dot();
//		Delay<DCollection.DIterator<String>> delay = d.exec(c.getIdentifier(), "iterator");
//		Identifier current = delay.get().current(d);
//		for (int i = 0; i < 10; i++) {
//			delay = d.exec(c.getIdentifier(), "iterator", current);
//			if (!delay.get().hasNext(d))
//				throw new RuntimeException("too small iterator!");
//			String next = delay.get().next(d);
//			if (!wanted.contains(next))
//				throw new RuntimeException("iterator returns bad data!");
//			wanted.remove(next);
//			current = delay.get().current(d);
//			dot();
//		}
//		if (delay.get().hasNext(d))
//			throw new RuntimeException("too big iterator!");
//		if (wanted.size() > 0)
//			throw new RuntimeException("didnt get all elements!");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void checkContent(Dhasher d, DCollection c, Collection<String> comparison) {
//		Set<String> wanted = new HashSet<String>(comparison);
//		if (!d.exec(c.getIdentifier(), "size").get().equals(new Long(wanted.size())))
//			throw new RuntimeException("wrong size! wanted " + wanted.size() + " but got " + d.exec(c.getIdentifier(), "size").get());
//
//		Delay<DCollection.DIterator<String>> delay = d.exec(c.getIdentifier(), "iterator");
//		for (int i = 0; i < comparison.size(); i++) {
//			if (!delay.get().hasNext(d))
//				throw new RuntimeException("too small iterator!");
//			String next = delay.get().next(d);
//			if (!wanted.contains(next))
//				throw new RuntimeException("iterator returns bad data! returned " + next + " but i expected one of " + wanted);
//			wanted.remove(next);
//			dot();
//		}
//		if (delay.get().hasNext(d))
//			throw new RuntimeException("too big iterator!");
//		if (wanted.size() > 0)
//			throw new RuntimeException("didnt get all elements!");
//
//		wanted = new HashSet<String>(comparison);
//		for (int i = 0; i < comparison.size(); i++) {
//			if (!delay.get().hasPrevious(d))
//				throw new RuntimeException("too small iterator! (backwards)");
//			String next = delay.get().previous(d);
//			if (!wanted.contains(next))
//				throw new RuntimeException("iterator returns bad data!");
//			wanted.remove(next);
//			dot();
//		}
//		if (delay.get().hasPrevious(d))
//			throw new RuntimeException("too big iterator! (backwards)");
//		if (wanted.size() > 0)
//			throw new RuntimeException("didnt get all elements!");
//
//	}
//
//	public static void testIteratorRemoval(Dhasher d, DCollection c) throws Exception {
//		System.out.print("\t\tTesting removal through iterator");
//		Set<String> wanted = new HashSet<String>();
//		for (int i = 0; i < 10; i++) {
//			wanted.add("blah" + i);
//		}
//		dot();
//		Delay<DCollection.DIterator<String>> delay = d.exec(c.getIdentifier(), "iterator");
//		for (int i = 0; i < 10; i++) {
//			checkContent(d, c, wanted);
//			if (!delay.get().hasNext(d))
//				throw new RuntimeException("too small iterator!");
//			String next = delay.get().next(d);
//			if (!wanted.contains(next))
//				throw new RuntimeException("iterator returns bad data");
//			delay.get().remove(d);
//			wanted.remove(next);
//			dot();
//		}
//		if (delay.get().hasNext(d))
//			throw new RuntimeException("too big iterator!");
//		if (!d.exec(c.getIdentifier(), "size").get().equals(new Long(0)))
//			throw new RuntimeException("remove didnt work!");
//		System.out.println("done!");
//	}
//
//	public static void testIteratorAdd(Dhasher d, DCollection c) throws Exception {
//		System.out.print("\t\tTesting adding through iterator");
//		Set<String> wanted = new HashSet<String>();
//		Delay<DCollection.DIterator<String>> delay = d.exec(c.getIdentifier(), "iterator");
//		for (int i = 0; i < 10; i++) {
//			delay.get().add(d, "blah" + i);
//			wanted.add("blah" + i);
//			checkContent(d, c, wanted);
//		}
//		System.out.println("done!");
//	}
//
//	public static void testRemoval(Dhasher d, DCollection c) throws Exception {
//		System.out.print("\t\tTesting element removal");
//		for (int i = 0; i < 10; i++) {
//			d.exec(c.getIdentifier(), "add", "blah" + i).get();
//			dot();
//		}
//		for (int i = 0; i < 10; i++) {
//			if (!d.exec(c.getIdentifier(), "contains", "blah" + i).get().equals(Boolean.TRUE))
//				throw new RuntimeException("missing element!");
//			dot();
//			d.exec(c.getIdentifier(), "remove", "blah" + i).get();
//			dot();
//			if (!d.exec(c.getIdentifier(), "contains", "blah" + i).get().equals(Boolean.FALSE))
//				throw new RuntimeException("remove failed!");
//			dot();
//		}
//		if (!d.exec(c.getIdentifier(), "size").get().equals(new Long(0)))
//			throw new RuntimeException("remove failed!");
//		System.out.println("done!");
//	}
//
//	public static void testCollectionRemoval(Dhasher d, DCollection c) throws Exception {
//		System.out.print("\t\tTesting collection removal");
//		if (!d.exec(c.getIdentifier(), "size").get().equals(new Long(0)))
//			throw new RuntimeException("collection is not empty!");
//
//		setEnableSynchronize(false);
//
//		Thread.sleep(500);
//
//		destroy();
//		ensureEmpty();
//		dot();
//		d.put(c).get();
//		dot();
//		for (int i = 0; i < 10; i++) {
//			d.exec(c.getIdentifier(), "add", "blapp" + i).get();
//			dot();
//		}
//		d.exec(c.getIdentifier(), "remove").get();
//		dot();
//		ensureEmpty();
//		dot();
//
//		setEnableSynchronize(true);
//		System.out.println("done!");
//	}
//
//	public static void testDCollection(Dhasher d, DCollection c) throws Exception {
//		d.put(c).get();
//		testAddSize(d, c);
//		testIteratorAdd(d, c);
//		testIterator(d, c);
//		testContinuedIterator(d, c);
//		testIteratorRemoval(d, c);
//		testRemoval(d, c);
//		testCollectionRemoval(d, c);
//	}
//
//	public static void testDList(Dhasher d) throws Exception {
//		System.out.println("\tTesting DList");
//		DCollection c = new DList();
//		testDCollection(d, c);
//	}
//
//	public static void testDSet(Dhasher d) throws Exception {
//		System.out.println("\tTesting DSet");
//		DCollection c = new DSet();
//		testDCollection(d, c);
//	}
//
//	public static void testDSortedSet(Dhasher d) throws Exception {
//		System.out.println("\tTesting DSortedSet");
//		DCollection c = new DSortedSet();
//		testDCollection(d, c);
//	}
//
//	public static void testDIndexedList(Dhasher d) throws Exception {
//		System.out.println("\tTesting DIndexedList");
//		DCollection c = new DIndexedList();
//		testDCollection(d, c);
//	}
//
//	public static void testDTree(Dhasher d) throws Exception {
//		System.out.println("\tTesting DTree");
//		DCollection c = new DTree();
//		testDCollection(d, c);
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		System.out.println("Testing DCollections");
//		Dhasher d = new Dhasher(dhash1);
//		testDSortedSet(d);
//		testDIndexedList(d);
//		testDList(d);
//		testDSet(d);
//		testDTree(d);
//	}
//
//}