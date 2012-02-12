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
//import java.util.List;
//
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.structures.DList;
//import cx.ath.troja.nja.Identifier;
//import cx.ath.troja.nja.Log;
//
//public class TestDList extends TestDHash {
//
//	public static List<String> testDListAppendPrepend(Dhasher d, DList<String> list) {
//		System.out.print("\tTesting DList append/prepend");
//
//		d.destroy().get();
//		dot();
//
//		d.put(list).get();
//
//		int n = 20;
//
//		List<String> comparison = new ArrayList<String>();
//
//		for (int i = 0; i < n; i++) {
//			Delay<Long> sizeDelay = d.exec(list.getIdentifier(), "size");
//			if (sizeDelay.get().intValue() != i * 2)
//				throw new RuntimeException("wrong size!");
//			String newElement = "element" + i;
//			d.exec(list.getIdentifier(), "append", newElement).get();
//			d.exec(list.getIdentifier(), "prepend", newElement).get();
//			comparison.add(newElement);
//			comparison.add(0, newElement);
//			dot();
//		}
//
//		dot();
//
//		verifyList(d, list, comparison);
//
//		System.out.println("done!");
//		return comparison;
//	}
//
//	public static void testDListInject(Dhasher d, DList<String> list, List<String> comparison) {
//		System.out.print("\tTesting DList inject");
//		int l = 0;
//		for (String s : comparison) {
//			l += s.length();
//		}
//		Delay<Integer> lengthDelay = d.exec(list.getIdentifier(), "inject", new Integer(0), new DList.Inject<Integer, String>() {
//			public Integer inject(Integer sum, String el) {
//				dot();
//				return sum + el.length();
//			}
//		});
//		if (lengthDelay.get().intValue() != l)
//			throw new RuntimeException("bad result from injection");
//		System.out.println("done!");
//	}
//
//	public static void verifyList(Dhasher d, DList<String> list, List<String> comparison) {
//		d.exec(list.getIdentifier(), "verify").get();
//
//		List<Object> content = getContent(d, list);
//
//		if (!content.equals(comparison)) {
//			throw new RuntimeException("expected " + comparison + ", but got " + content);
//		}
//	}
//
//	public static List<String> testDListDirectElementRemoval(Dhasher d, DList<String> list, List<String> comparison) {
//		System.out.print("\tTesting DList direct element removal");
//
//		Delay<Identifier> di1 = d.exec(list.getIdentifier(), "append", "plupp");
//		di1.get();
//		comparison.add("plupp");
//		dot();
//		Delay<Identifier> di2 = d.exec(list.getIdentifier(), "prepend", "plupp2");
//		di2.get();
//		comparison.add(0, "plupp2");
//		dot();
//		Delay<Identifier> di3 = d.exec(list.getIdentifier(), "prepend", "plupp3");
//		di3.get();
//		comparison.add(0, "plupp3");
//		dot();
//
//		verifyList(d, list, comparison);
//		dot();
//
//		d.exec(di1.get(), "remove").get();
//		comparison.remove("plupp");
//		dot();
//
//		verifyList(d, list, comparison);
//		dot();
//
//		d.exec(di2.get(), "remove").get();
//		comparison.remove("plupp2");
//		dot();
//
//		verifyList(d, list, comparison);
//		dot();
//
//		d.exec(di3.get(), "remove").get();
//		comparison.remove("plupp3");
//		dot();
//
//		verifyList(d, list, comparison);
//		dot();
//
//		if (d.get(di1.get()).get() != null)
//			throw new RuntimeException("element removal failed!");
//
//		if (d.get(di2.get()).get() != null)
//			throw new RuntimeException("element removal failed!");
//
//		if (d.get(di3.get()).get() != null)
//			throw new RuntimeException("element removal failed!");
//
//		System.out.println("done!");
//		return comparison;
//	}
//
//	public static void testDListClear(Dhasher d, DList<String> list) {
//		System.out.print("\tTesting DList clearing");
//		List<DList.Element<String>> elements = getElements(d, list);
//
//		d.exec(list.getIdentifier(), "clear").get();
//		dot();
//		verifyList(d, list, new ArrayList<String>());
//		dot();
//
//		for (DList.Element<String> element : elements) {
//			if (d.get(element.getIdentifier()).get() != null) {
//				throw new RuntimeException("element " + element.getValue() + " not removed!");
//			}
//			dot();
//		}
//
//		System.out.println("done!");
//	}
//
//	public static void testDListRemove(Dhasher d, DList<String> list) throws Exception {
//		System.out.print("\tTesting DList removal");
//		setEnableSynchronize(false);
//
//		Thread.sleep(500);
//
//		destroy();
//		ensureEmpty();
//		dot();
//
//		d.put(list).get();
//		for (int i = 0; i < 10; i++) {
//			d.exec(list.getIdentifier(), "append", "hepp" + i).get();
//			d.exec(list.getIdentifier(), "prepend", "happ" + i).get();
//			dot();
//		}
//		List<DList.Element<String>> elements = getElements(d, list);
//		dot();
//		d.exec(list.getIdentifier(), "remove").get();
//		dot();
//		if (d.get(list.getIdentifier()).get() != null)
//			throw new RuntimeException("list not removed!");
//		for (DList.Element<String> element : elements) {
//			if (d.get(element.getIdentifier()).get() != null) {
//				throw new RuntimeException("element " + element.getValue() + " not removed!");
//			}
//			dot();
//		}
//
//		ensureEmpty();
//
//		d.put(list).get();
//		dot();
//		verifyList(d, list, new ArrayList<String>());
//		dot();
//
//		setEnableSynchronize(true);
//		System.out.println("done!");
//	}
//
//	public static void testDListElementInsertion(Dhasher d, DList<String> list) {
//		List<String> comparison = new ArrayList<String>();
//		System.out.print("\tTesting DList element insertion");
//		for (int i = 1; i < 4; i++) {
//			d.exec(list.getIdentifier(), "append", "apa" + i).get();
//			comparison.add("apa" + i);
//			dot();
//		}
//		verifyList(d, list, comparison);
//		d.exec(list.getIdentifier(), "each", new DList.Each<String>() {
//			public void each(String s) {
//				if (s.equals("apa1")) {
//					prependAsync("apa0").get();
//					appendAsync("apa1.1").get();
//				} else if (s.equals("apa2")) {
//					prependAsync("apa1.9").get();
//					appendAsync("apa2.1").get();
//				} else if (s.equals("apa3")) {
//					prependAsync("apa2.9").get();
//					appendAsync("apa3.1").get();
//				}
//				dot();
//			}
//		}).get();
//		comparison.add(0, "apa0");
//		comparison.add(2, "apa1.1");
//		comparison.add(3, "apa1.9");
//		comparison.add(5, "apa2.1");
//		comparison.add(6, "apa2.9");
//		comparison.add(8, "apa3.1");
//
//		verifyList(d, list, comparison);
//		System.out.println("done!");
//	}
//
//	public static void testDListElementAt(Dhasher d) {
//		System.out.print("\tTesting DList elementAt");
//		Log.getLogger(cx.ath.troja.chordless.dhash.storage.JDBCStorage.class).setLevel(Log.ERROR);
//		DList l = new DList();
//		d.put(l).get();
//		dot();
//		d.exec(l.getIdentifier(), "append", "a").get();
//		dot();
//		d.exec(l.getIdentifier(), "append", "b").get();
//		dot();
//		d.exec(l.getIdentifier(), "append", "c").get();
//		dot();
//		d.exec(l.getIdentifier(), "append", "d").get();
//		dot();
//		Delay<DList.Element> element = d.exec(l.getIdentifier(), "elementAt", new Long(0));
//		if (!element.get().getValue().equals("a"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(1));
//		if (!element.get().getValue().equals("b"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(2));
//		if (!element.get().getValue().equals("c"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(3));
//		if (!element.get().getValue().equals("d"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		try {
//			d.exec(l.getIdentifier(), "elementAt", new Long(4)).get();
//			throw new RuntimeException("should be exception!");
//		} catch (IndexOutOfBoundsException e) {
//			dot();
//		}
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(-1));
//		if (!element.get().getValue().equals("d"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(-2));
//		if (!element.get().getValue().equals("c"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(-3));
//		if (!element.get().getValue().equals("b"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		element = d.exec(l.getIdentifier(), "elementAt", new Long(-4));
//		if (!element.get().getValue().equals("a"))
//			throw new RuntimeException("bad element at index");
//		dot();
//		try {
//			d.exec(l.getIdentifier(), "elementAt", new Long(-5)).get();
//			throw new RuntimeException("should be exception!");
//		} catch (IndexOutOfBoundsException e) {
//			dot();
//		}
//		System.out.println("done!");
//	}
//
//	public static void testDListFirstLast(Dhasher d) throws Exception {
//		System.out.print("\tTesting DList first/last");
//		DList l = new DList();
//		d.put(l).get();
//		dot();
//		for (int i = 0; i < 5; i++) {
//			d.exec(l.getIdentifier(), "append", "last" + i).get();
//			d.exec(l.getIdentifier(), "prepend", "first" + i).get();
//			if (!d.exec(l.getIdentifier(), "first").get().equals("first" + i))
//				throw new RuntimeException("wrong first!");
//			if (!d.exec(l.getIdentifier(), "last").get().equals("last" + i))
//				throw new RuntimeException("wrong last!");
//			dot();
//		}
//		for (int i = 4; i >= 0; i--) {
//			Delay<String> delay = d.exec(l.getIdentifier(), "removeFirst");
//			if (!delay.get().equals("first" + i))
//				throw new RuntimeException("wrong first! got " + delay.get() + " expected first" + i);
//			if (!d.exec(l.getIdentifier(), "removeLast").get().equals("last" + i))
//				throw new RuntimeException("wrong last!");
//			dot();
//		}
//		if (!d.exec(l.getIdentifier(), "size").get().equals(new Long(0)))
//			throw new RuntimeException("remove didnt work!");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		test(arguments);
//		teardown();
//	}
//
//	public static List<String> testDListElementRemoval(Dhasher d, DList<String> list) {
//		System.out.print("\tTesting DList element removal");
//
//		List<DList.Element<String>> elements = getElements(d, list);
//
//		d.exec(list.getIdentifier(), "each", new DList.Each<String>() {
//			public void each(String s) {
//				remove();
//				dot();
//			}
//		}).get();
//		dot();
//
//		List<String> comparison = new ArrayList<String>();
//
//		verifyList(d, list, comparison);
//
//		for (DList.Element<String> element : elements) {
//			if (d.get(element.getIdentifier()).get() != null) {
//				throw new RuntimeException("element not removed!");
//			}
//			dot();
//		}
//
//		d.exec(list.getIdentifier(), "append", "first").get();
//		comparison.add("first");
//		dot();
//		d.exec(list.getIdentifier(), "append", "middle").get();
//		comparison.add("middle");
//		dot();
//		d.exec(list.getIdentifier(), "append", "last").get();
//		comparison.add("last");
//		dot();
//
//		verifyList(d, list, comparison);
//
//		d.exec(list.getIdentifier(), "each", new DList.Each<String>() {
//			public void each(String s) {
//				if (s.equals("first"))
//					remove();
//				dot();
//			}
//		}).get();
//		comparison.remove("first");
//		dot();
//
//		verifyList(d, list, comparison);
//
//		d.exec(list.getIdentifier(), "each", new DList.Each<String>() {
//			public void each(String s) {
//				if (s.equals("last"))
//					remove();
//				dot();
//			}
//		}).get();
//		comparison.remove("last");
//
//		verifyList(d, list, comparison);
//
//		d.exec(list.getIdentifier(), "prepend", "really first").get();
//		comparison.add(0, "really first");
//
//		verifyList(d, list, comparison);
//
//		System.out.println("done!");
//		return comparison;
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		if (arguments.length == 1 && arguments[0].equals("lr")) {
//			while (true) {
//				testDListRemove(new Dhasher(dhash1), new DList<String>());
//			}
//		} else if (arguments.length == 1 && arguments[0].equals("ap")) {
//			while (true) {
//				Dhasher d = new Dhasher(dhashes[0]);
//				DList<String> list = new DList<String>();
//				testDListAppendPrepend(d, list);
//			}
//		} else {
//			System.out.println("Testing DList");
//			Dhasher d = new Dhasher(dhashes[0]);
//			DList<String> list = new DList<String>();
//
//			testDListFirstLast(d);
//			List<String> comparison = testDListAppendPrepend(d, list);
//			testDListInject(d, list, comparison);
//			comparison = testDListElementRemoval(d, list);
//			comparison = testDListDirectElementRemoval(d, list, comparison);
//			testDListClear(d, list);
//			testDListElementInsertion(d, list);
//			testDListRemove(d, list);
//			testDListElementAt(d);
//		}
//	}
//
//}
