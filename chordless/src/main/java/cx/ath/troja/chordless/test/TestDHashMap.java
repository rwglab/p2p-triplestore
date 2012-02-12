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
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import cx.ath.troja.chordless.dhash.DHash;
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.structures.DHashMap;
//import cx.ath.troja.chordless.dhash.structures.DMap;
//import cx.ath.troja.chordless.dhash.transactions.Transaction;
//import cx.ath.troja.nja.Identifier;
//
//public class TestDHashMap extends TestDHash {
//
//	public static void verifyMap(Dhasher d, DHashMap<String, String> map, Map<String, String> comparison) {
//		Delay<Long> sizeDelay = d.exec(map.getIdentifier(), "size");
//		if (comparison.size() != sizeDelay.get().intValue())
//			throw new RuntimeException("wrong size! got " + sizeDelay.get() + " but expected " + comparison.size());
//		Delay<Map<String, String>> contentDelay = d.exec(map.getIdentifier(), "inject", new HashMap<String, String>(),
//				new DMap.DefaultDInject<Map<String, String>, String, String>() {
//					public Map<String, String> inject(Map<String, String> sum, Map.Entry<String, String> entry) {
//						sum.put(entry.getKey(), entry.getValue());
//						return sum;
//					}
//				});
//		if (!contentDelay.get().equals(comparison))
//			throw new RuntimeException("bad content! expected " + comparison + " but got " + contentDelay.get());
//	}
//
//	public static List<DMap.DMapEntry<String, String>> getEntries(Dhasher d, DHashMap<String, String> map) {
//		Delay<List<DMap.DMapEntry<String, String>>> entryDelay = d.exec(map.getIdentifier(), "inject",
//				new ArrayList<DMap.DMapEntry<String, String>>(), new DMap.DefaultDInject<List<DMap.DMapEntry<String, String>>, String, String>() {
//					public List<DMap.DMapEntry<String, String>> inject(List<DMap.DMapEntry<String, String>> sum, Map.Entry<String, String> e) {
//						sum.add(getEntry());
//						return sum;
//					}
//				});
//		return entryDelay.get();
//	}
//
//	public static Map<String, String> testDHashMapPutGetAtDel(Dhasher d, DHashMap<String, String> map) {
//		System.out.print("\tTesting DHashMap put/get/del");
//		long t = System.currentTimeMillis();
//
//		Map<String, String> comparison = new HashMap<String, String>();
//		d.put(map).get();
//
//		dot();
//		for (int i = 0; i < 20; i++) {
//			if (d.exec(map.getIdentifier(), "put", "key" + i, "value" + i).get() != null)
//				throw new RuntimeException("wrong data from put");
//			comparison.put("key" + i, "value" + i);
//			dot();
//		}
//		verifyMap(d, map, comparison);
//		dot();
//
//		for (int i = 0; i < 20; i++) {
//			Delay<String> delay = d.exec(map.getIdentifier(), "get", "key" + i);
//			if (!delay.get().equals("value" + i))
//				throw new RuntimeException("wrong data from get!");
//			dot();
//		}
//
//		for (int i = 0; i < 20; i++) {
//			Delay<String> delay = d.exec(map.getIdentifier(), "del", "key" + i);
//			if (!delay.get().equals("value" + i))
//				throw new RuntimeException("wrong data from del!");
//			comparison.remove("key" + i);
//			dot();
//		}
//
//		verifyMap(d, map, comparison);
//
//		System.out.println("done! " + (System.currentTimeMillis() - t));
//
//		return comparison;
//	}
//
//	public static void testDHashMapInject(Dhasher d, DHashMap<String, String> map, Map<String, String> comparison) {
//		System.out.print("\tTesting DHashMap inject");
//		long t = System.currentTimeMillis();
//
//		verifyMap(d, map, comparison);
//		for (int i = 0; i < 10; i++) {
//			d.exec(map.getIdentifier(), "put", "key" + i, "value" + i).get();
//			comparison.put("key" + i, "value" + i);
//			dot();
//		}
//		int realsize = 0;
//		for (Map.Entry<String, String> entry : comparison.entrySet()) {
//			realsize += entry.getValue().length();
//		}
//		dot();
//		verifyMap(d, map, comparison);
//		Delay<Integer> delay = d.exec(map.getIdentifier(), "inject", new Integer(0), new DMap.DefaultDInject<Integer, String, String>() {
//			public Integer inject(Integer sum, Map.Entry<String, String> entry) {
//				dot();
//				return sum + entry.getValue().length();
//			}
//		});
//		if (realsize != delay.get().intValue())
//			throw new RuntimeException("bad inject return, got " + delay.get() + " but expected " + realsize);
//
//		System.out.println("done! " + (System.currentTimeMillis() - t));
//	}
//
//	public static void testDHashMapEntryRemoval(Dhasher d, DHashMap<String, String> map, Map<String, String> comparison) {
//		System.out.print("\tTesting DHashMap entry removal");
//		long t = System.currentTimeMillis();
//
//		List<DMap.DMapEntry<String, String>> entries = getEntries(d, map);
//
//		verifyMap(d, map, comparison);
//		d.exec(map.getIdentifier(), "each", new DMap.DefaultDEach<String, String>() {
//			public void each(Map.Entry<String, String> entry) {
//				remove();
//				dot();
//			}
//		}).get();
//		comparison.clear();
//
//		verifyMap(d, map, comparison);
//
//		for (DMap.DMapEntry<String, String> entry : entries) {
//			if (d.get(entry.getIdentifier()).get() != null)
//				throw new RuntimeException("entry not removed!");
//		}
//
//		System.out.println("done! " + (System.currentTimeMillis() - t));
//	}
//
//	public static void testDHashMapClear(Dhasher d, DHashMap<String, String> map) {
//		System.out.print("\tTesting DHashMap clearing");
//		long t = System.currentTimeMillis();
//
//		Map<String, String> comparison = new HashMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			d.exec(map.getIdentifier(), "put", "key" + i, "value" + i).get();
//			comparison.put("key" + i, "value" + i);
//			dot();
//		}
//
//		verifyMap(d, map, comparison);
//
//		List<DMap.DMapEntry<String, String>> entries = getEntries(d, map);
//
//		d.exec(map.getIdentifier(), "clear").get();
//		comparison.clear();
//
//		verifyMap(d, map, comparison);
//
//		for (DMap.DMapEntry<String, String> entry : entries) {
//			if (d.get(entry.getIdentifier()).get() != null)
//				throw new RuntimeException("entry not removed!");
//		}
//
//		System.out.println("done! " + (System.currentTimeMillis() - t));
//	}
//
//	public static void testDHashMapRemove(Dhasher d, DHashMap<String, String> map) throws Exception {
//		System.out.print("\tTesting DHashMap removal");
//		long t = System.currentTimeMillis();
//
//		setEnableSynchronize(false);
//
//		Thread.sleep(500);
//
//		destroy();
//		ensureEmpty();
//		dot();
//
//		d.put(map).get();
//
//		Map<String, String> comparison = new HashMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			d.exec(map.getIdentifier(), "put", "key" + i, "value" + i).get();
//			comparison.put("key" + i, "value" + i);
//			dot();
//		}
//
//		verifyMap(d, map, comparison);
//
//		List<DMap.DMapEntry<String, String>> entries = getEntries(d, map);
//
//		d.exec(map.getIdentifier(), "remove").get();
//
//		if (d.get(map.getIdentifier()).get() != null)
//			throw new RuntimeException("map not removed!");
//		for (DMap.DMapEntry<String, String> entry : entries) {
//			if (d.get(entry.getIdentifier()).get() != null)
//				throw new RuntimeException("entry not removed!");
//		}
//
//		ensureEmpty();
//
//		setEnableSynchronize(true);
//		System.out.println("done! " + (System.currentTimeMillis() - t));
//	}
//
//	public static void testDHashMapTransactions(Dhasher d) {
//		System.out.print("\tTesting DHashMap transaction semantics");
//
//		Map<String, String> comp = new HashMap<String, String>();
//		DHashMap<String, String> map = new DHashMap<String, String>();
//		d.put(map).get();
//		dot();
//		Delay<Identifier> delay1 = d.exec(map.getIdentifier(), "put", "key1", "value1");
//		comp.put("key1", "value1");
//		delay1.get();
//		dot();
//		Delay<Identifier> delay2 = d.exec(map.getIdentifier(), "put", "key2", "value2");
//		comp.put("key2", "value2");
//		delay2.get();
//		dot();
//		Delay<Identifier> delay3 = d.exec(map.getIdentifier(), "put", "key3", "value3");
//		comp.put("key3", "value3");
//		delay3.get();
//		dot();
//		if (!comp.equals(new HashMap<Object, Object>(getContent(d, map))))
//			throw new RuntimeException("not right content");
//		dot();
//		Transaction t1 = d.transaction();
//		Transaction t2 = d.transaction();
//		t1.exec(map.getIdentifier(), "put", "transkey1", "transvalue1").get();
//		dot();
//		t2.exec(map.getIdentifier(), "put", "transkey2", "transvalue2").get();
//		dot();
//		t1.commit();
//		dot();
//		t2.commit();
//		dot();
//		comp.put("transkey1", "transvalue1");
//		comp.put("transkey2", "transvalue2");
//		if (!comp.equals(new HashMap<Object, Object>(getContent(d, map))))
//			throw new RuntimeException("not right content");
//		dot();
//		Transaction t3 = d.transaction();
//		t1 = t3.transaction();
//		t2 = t3.transaction();
//		t1.exec(map.getIdentifier(), "put", "transkey11", "transvalue11").get();
//		dot();
//		t2.exec(map.getIdentifier(), "put", "transkey22", "transvalue22").get();
//		dot();
//		t1.commit();
//		dot();
//		t2.commit();
//		dot();
//		t3.commit();
//		dot();
//		comp.put("transkey11", "transvalue11");
//		comp.put("transkey22", "transvalue22");
//		if (!comp.equals(new HashMap<Object, Object>(getContent(d, map))))
//			throw new RuntimeException("not right content");
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		long t = System.currentTimeMillis();
//
//		test(arguments);
//
//		System.out.println("Total: " + (System.currentTimeMillis() - t));
//		teardown();
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		DHash dhash = dhash1;
//		System.out.println("Testing DHashMap");
//		Dhasher d = new Dhasher(dhash);
//		DHashMap<String, String> map = new DHashMap<String, String>();
//
//		Map<String, String> comparison = testDHashMapPutGetAtDel(d, map);
//		testDHashMapTransactions(d);
//		testDHashMapInject(d, map, comparison);
//		testDHashMapEntryRemoval(d, map, comparison);
//		testDHashMapClear(d, map);
//		testDHashMapRemove(d, new DHashMap<String, String>());
//	}
//
//}
