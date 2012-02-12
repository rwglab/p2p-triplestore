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
//import java.util.HashMap;
//import java.util.Map;
//
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.structures.DHashMap;
//import cx.ath.troja.chordless.dhash.structures.DMap;
//import cx.ath.troja.chordless.dhash.structures.DTreap;
//import cx.ath.troja.nja.Identifier;
//
//public class TestDMaps extends TestDHash {
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		test(arguments);
//		teardown();
//	}
//
//	public static void testPutSizeGetDel(Dhasher d, DMap m) throws Exception {
//		System.out.print("\t\tTesting put/size/get/inject/del");
//		Map<String, String> cmp = new HashMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			if (!d.exec(m.getIdentifier(), "size").get().equals(new Long(i)))
//				throw new RuntimeException("wrong size!");
//			d.exec(m.getIdentifier(), "put", "key" + i, "value" + i).get();
//			cmp.put("key" + i, "value" + i);
//			Delay<Map<String, String>> injectDelay = d.exec(m.getIdentifier(), "inject", new HashMap<String, String>(),
//					new DMap.DefaultDInject<Map<String, String>, String, String>() {
//						public Map<String, String> inject(Map<String, String> sum, Map.Entry<String, String> e) {
//							sum.put(e.getKey(), e.getValue());
//							return sum;
//						}
//					});
//			if (!injectDelay.get().equals(cmp))
//				throw new RuntimeException("inject did not collect the right entries! got " + injectDelay.get() + " but wanted " + cmp);
//
//			dot();
//		}
//		for (int i = 0; i < 10; i++) {
//			if (!d.exec(m.getIdentifier(), "get", "key" + i).get().equals("value" + i))
//				throw new RuntimeException("wrong value!");
//			dot();
//		}
//		for (int i = 0; i < 10; i++) {
//			if (!d.exec(m.getIdentifier(), "size").get().equals(new Long(10 - i)))
//				throw new RuntimeException("wrong size!");
//			d.exec(m.getIdentifier(), "del", "key" + i).get();
//			dot();
//		}
//		System.out.println("done!");
//	}
//
//	public static void testIterator(Dhasher d, DMap m) throws Exception {
//		System.out.print("\t\tTesting entryIterator");
//		Map<String, String> cmp = new HashMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			cmp.put("KEY" + i, "VALUE" + i);
//			d.exec(m.getIdentifier(), "put", "KEY" + i, "VALUE" + i).get();
//			dot();
//		}
//		Delay<DMap.DEntryIterator<String, String>> delay = d.exec(m.getIdentifier(), "entryIterator");
//		for (int i = 0; i < 10; i++) {
//			if (!delay.get().hasNext(d))
//				throw new RuntimeException("too small iterator!");
//			Map.Entry<String, String> entry = delay.get().next(d);
//			if (!cmp.containsKey(entry.getKey()))
//				throw new RuntimeException("bad key from iterator");
//			if (!cmp.get(entry.getKey()).equals(entry.getValue()))
//				throw new RuntimeException("bad value from iterator, got " + entry.getValue() + " but expected " + cmp.get(entry.getKey())
//						+ " since key is " + entry.getKey());
//			cmp.remove(entry.getKey());
//			dot();
//		}
//		if (delay.get().hasNext(d))
//			throw new RuntimeException("too big iterator!");
//		dot();
//		if (cmp.size() > 0)
//			throw new RuntimeException("didnt get all data from iterator!");
//		dot();
//
//		for (int i = 0; i < 10; i++) {
//			cmp.put("KEY" + i, "VALUE" + i);
//		}
//		for (int i = 0; i < 10; i++) {
//			if (!delay.get().hasPrevious(d))
//				throw new RuntimeException("too small iterator! (backwards)");
//			Map.Entry<String, String> entry = delay.get().previous(d);
//			if (!cmp.containsKey(entry.getKey()))
//				throw new RuntimeException("bad key from iterator");
//			if (!cmp.get(entry.getKey()).equals(entry.getValue()))
//				throw new RuntimeException("bad value from iterator, got " + entry.getValue() + " but expected " + cmp.get(entry.getKey())
//						+ " since key is " + entry.getKey());
//			cmp.remove(entry.getKey());
//			dot();
//		}
//		if (delay.get().hasPrevious(d))
//			throw new RuntimeException("too big iterator! (backwards)");
//		dot();
//		if (cmp.size() > 0)
//			throw new RuntimeException("didnt get all data from iterator!");
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static void testContinuedIterator(Dhasher d, DMap m) throws Exception {
//		System.out.print("\t\tTesting continued iterators");
//		Map<String, String> cmp = new HashMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			cmp.put("KEY" + i, "VALUE" + i);
//		}
//		dot();
//		Delay<DMap.DEntryIterator<String, String>> delay = d.exec(m.getIdentifier(), "entryIterator");
//		Identifier current = delay.get().current(d);
//		for (int i = 0; i < 10; i++) {
//			delay = d.exec(m.getIdentifier(), "entryIterator", current);
//			if (!delay.get().hasNext(d))
//				throw new RuntimeException("too small iterator!");
//			Map.Entry<String, String> entry = delay.get().next(d);
//			if (!cmp.containsKey(entry.getKey()))
//				throw new RuntimeException("bad key from iterator");
//			if (!cmp.get(entry.getKey()).equals(entry.getValue()))
//				throw new RuntimeException("bad value from iterator, got " + entry.getValue() + " but expected " + cmp.get(entry.getKey())
//						+ " since key is " + entry.getKey());
//			cmp.remove(entry.getKey());
//			current = delay.get().current(d);
//			dot();
//		}
//		if (delay.get().hasNext(d))
//			throw new RuntimeException("too big iterator!");
//		dot();
//		if (cmp.size() > 0)
//			throw new RuntimeException("didnt get all data from iterator!");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void testIteratorRemoval(Dhasher d, DMap m) throws Exception {
//		System.out.print("\t\tTesting removal through iterator");
//		Delay<DMap.DEntryIterator<String, String>> delay = d.exec(m.getIdentifier(), "entryIterator");
//		dot();
//		for (int i = 0; i < 10; i++) {
//			if (!d.exec(m.getIdentifier(), "size").get().equals(new Long(10 - i)))
//				throw new RuntimeException("wrong size!");
//			if (!delay.get().hasNext(d))
//				throw new RuntimeException("too smal iterator!");
//			Map.Entry<String, String> entry = delay.get().next(d);
//			delay.get().remove(d);
//			dot();
//		}
//		if (delay.get().hasNext(d))
//			throw new RuntimeException("too big iterator!");
//		dot();
//		if (!d.exec(m.getIdentifier(), "size").get().equals(new Long(0)))
//			throw new RuntimeException("remove didnt work!");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static void testRemoval(Dhasher d, DMap m) throws Exception {
//		System.out.print("\t\tTesting removal");
//		if (!d.exec(m.getIdentifier(), "size").get().equals(new Long(0)))
//			throw new RuntimeException("map is not empty!");
//
//		setEnableSynchronize(false);
//
//		Thread.sleep(500);
//
//		destroy();
//		ensureEmpty();
//		dot();
//
//		d.put(m).get();
//		dot();
//		for (int i = 0; i < 10; i++) {
//			d.exec(m.getIdentifier(), "put", "k" + i, "v" + i).get();
//			dot();
//		}
//		d.exec(m.getIdentifier(), "remove").get();
//		dot();
//		ensureEmpty();
//		dot();
//
//		setEnableSynchronize(true);
//		System.out.println("done!");
//	}
//
//	public static void testDMap(Dhasher d, DMap m) throws Exception {
//		d.put(m).get();
//		testPutSizeGetDel(d, m);
//		testIterator(d, m);
//		testContinuedIterator(d, m);
//		testIteratorRemoval(d, m);
//		testRemoval(d, m);
//	}
//
//	public static void testDHashMap(Dhasher d) throws Exception {
//		System.out.println("\tTesting DHashMap");
//		DMap m = new DHashMap();
//		testDMap(d, m);
//	}
//
//	public static void testDTreap(Dhasher d) throws Exception {
//		System.out.println("\tTesting DTreap");
//		DMap m = new DTreap();
//		testDMap(d, m);
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		System.out.println("Testing DMaps");
//		Dhasher d = new Dhasher(dhash1);
//		testDTreap(d);
//		testDHashMap(d);
//	}
//
//}