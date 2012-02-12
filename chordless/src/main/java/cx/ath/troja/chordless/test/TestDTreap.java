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
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.SortedMap;
//import java.util.TreeMap;
//
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.structures.DMap;
//import cx.ath.troja.chordless.dhash.structures.DTreap;
//
//public class TestDTreap extends TestDHash {
//
//	public static void verifyTreap(Dhasher d, DTreap t, SortedMap<String, String> ref) {
//		Delay<Long> sizeDelay = d.exec(t.getIdentifier(), "size");
//		if (ref.size() != sizeDelay.get().intValue())
//			throw new RuntimeException("wrong size! got " + sizeDelay.get() + " but expected " + ref.size());
//		Delay<SortedMap<String, String>> contentDelay = d.exec(t.getIdentifier(), "inject", new TreeMap<String, String>(),
//				new DMap.DefaultDInject<SortedMap<String, String>, String, String>() {
//					public SortedMap<String, String> inject(SortedMap<String, String> sum, Map.Entry<String, String> entry) {
//						sum.put(entry.getKey(), entry.getValue());
//						return sum;
//					}
//				});
//		if (!contentDelay.get().equals(ref))
//			throw new RuntimeException("bad content! expected " + ref + " but got " + contentDelay.get());
//
//	}
//
//	@SuppressWarnings("unchecked")
//	public static void testPutGetAtDel() {
//		System.out.print("\tTesting DTreap put/get/del");
//		Dhasher d = new Dhasher(dhash1);
//		dot();
//		DTreap t = new DTreap();
//		dot();
//		d.put(t).get();
//		SortedMap<String, String> ref = new TreeMap<String, String>();
//		List<List<String>> refList = new ArrayList<List<String>>();
//		for (int i = 0; i < 10; i++) {
//			String k = "" + Math.random();
//			String v = "" + Math.random();
//			ArrayList<String> entry = new ArrayList<String>();
//			entry.add(k);
//			entry.add(v);
//			refList.add(entry);
//			if (d.exec(t.getIdentifier(), "put", k, v).get() != null)
//				throw new RuntimeException("put returned non null");
//			ref.put(k, v);
//			dot();
//			Map.Entry<String, String> first = (Map.Entry<String, String>) d.exec(t.getIdentifier(), "getFirst").get();
//			if (!first.getKey().equals(ref.firstKey()))
//				throw new RuntimeException("getFirst failed! wanted " + ref.firstKey() + " but got " + first.getKey());
//			dot();
//			Map.Entry<String, String> last = (Map.Entry<String, String>) d.exec(t.getIdentifier(), "getLast").get();
//			if (!last.getKey().equals(ref.lastKey()))
//				throw new RuntimeException("getLast failed!");
//			dot();
//			verifyTreap(d, t, ref);
//			dot();
//		}
//		for (Map.Entry<String, String> entry : ref.entrySet()) {
//			Object v = d.exec(t.getIdentifier(), "get", entry.getKey()).get();
//			if (!v.equals(entry.getValue()))
//				throw new RuntimeException("get failed, expected " + entry.getValue() + " but got " + v);
//			dot();
//		}
//
//		Collections.sort(refList, new Comparator<List<String>>() {
//			public int compare(List<String> s1, List<String> s2) {
//				return s1.get(0).compareTo(s2.get(0));
//			}
//		});
//
//		Iterator<Map.Entry<String, String>> iter = ref.entrySet().iterator();
//		while (iter.hasNext()) {
//			Map.Entry<String, String> entry = iter.next();
//			if (!d.exec(t.getIdentifier(), "del", entry.getKey()).get().equals(entry.getValue()))
//				throw new RuntimeException("del returned wrong value");
//			iter.remove();
//			dot();
//			verifyTreap(d, t, ref);
//			dot();
//		}
//		for (Map.Entry<String, String> entry : ref.entrySet()) {
//			if (d.exec(t.getIdentifier(), "get", entry.getKey()).get() != null)
//				throw new RuntimeException("del failed");
//			dot();
//		}
//		System.out.println("done!");
//	}
//
//	public static void testInject() throws Exception {
//		System.out.print("\tTesting DTreap inject");
//		Dhasher d = new Dhasher(dhash3);
//		dot();
//		DTreap t = new DTreap();
//		d.put(t).get();
//		dot();
//		SortedMap<String, String> ref = new TreeMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			String k = "" + Math.random();
//			String v = "" + Math.random();
//			if (d.exec(t.getIdentifier(), "put", k, v).get() != null)
//				throw new RuntimeException("put returned non null");
//			ref.put(k, v);
//			dot();
//		}
//
//		verifyTreap(d, t, ref);
//
//		String refSum = "";
//		for (Map.Entry<String, String> entry : ref.entrySet()) {
//			refSum += "," + entry.getKey() + "=>" + entry.getValue();
//		}
//		dot();
//		Object treeSum = d.exec(t.getIdentifier(), "inject", "", new DMap.DefaultDInject<String, String, String>() {
//			public String inject(String sum, Map.Entry<String, String> entry) {
//				return sum + "," + entry.getKey() + "=>" + entry.getValue();
//			}
//		}).get();
//		if (!treeSum.equals(refSum))
//			throw new RuntimeException("wrong inject result! expected " + refSum + " but got " + treeSum);
//		dot();
//
//		refSum = "";
//		List<Map.Entry<String, String>> reverseEntries = new ArrayList<Map.Entry<String, String>>(ref.entrySet());
//		Collections.reverse(reverseEntries);
//		for (Map.Entry<String, String> entry : reverseEntries) {
//			refSum += "," + entry.getKey() + "=>" + entry.getValue();
//		}
//		dot();
//		treeSum = d.exec(t.getIdentifier(), "inject", "", new DMap.DefaultDInject<String, String, String>() {
//			public String inject(String sum, Map.Entry<String, String> entry) {
//				return sum + "," + entry.getKey() + "=>" + entry.getValue();
//			}
//		}, new Integer(-1)).get();
//		if (!treeSum.equals(refSum))
//			throw new RuntimeException("wrong inject result! expected " + refSum + " but got " + treeSum);
//		dot();
//
//		List<String> keys = new ArrayList<String>(ref.keySet());
//		final String middleKey = keys.get(keys.size() / 2);
//		refSum = "";
//		for (Map.Entry<String, String> entry : ref.tailMap(middleKey).entrySet()) {
//			refSum += "," + entry.getKey() + "=>" + entry.getValue();
//		}
//		treeSum = d.exec(t.getIdentifier(), "inject", "", new DMap.DefaultDInject<String, String, String>() {
//			public String inject(String sum, Map.Entry<String, String> entry) {
//				return sum + "," + entry.getKey() + "=>" + entry.getValue();
//			}
//		}, new Integer(1), middleKey).get();
//		if (!treeSum.equals(refSum))
//			throw new RuntimeException("wrong inject result! expected " + refSum + " but got " + treeSum);
//		dot();
//
//		refSum = "";
//		reverseEntries = new ArrayList<Map.Entry<String, String>>(ref.headMap(middleKey + "0").entrySet());
//		Collections.reverse(reverseEntries);
//		for (Map.Entry<String, String> entry : reverseEntries) {
//			refSum += "," + entry.getKey() + "=>" + entry.getValue();
//		}
//		treeSum = d.exec(t.getIdentifier(), "inject", "", new DMap.DefaultDInject<String, String, String>() {
//			public String inject(String sum, Map.Entry<String, String> entry) {
//				return sum + "," + entry.getKey() + "=>" + entry.getValue();
//			}
//		}, new Integer(-1), middleKey).get();
//		if (!treeSum.equals(refSum))
//			throw new RuntimeException("wrong inject result! expected " + refSum + " but got " + treeSum);
//		dot();
//
//		refSum = "";
//		for (Map.Entry<String, String> entry : ref.entrySet()) {
//			refSum += "," + entry.getKey() + "=>" + entry.getValue();
//			if (entry.getKey().equals(middleKey))
//				break;
//		}
//		treeSum = d.exec(t.getIdentifier(), "inject", "", new DTreap.Inject<String, String, String>() {
//			public String inject(String sum, Map.Entry<String, String> entry) {
//				String returnValue = sum + "," + entry.getKey() + "=>" + entry.getValue();
//				if (entry.getKey().equals(middleKey))
//					done();
//				return returnValue;
//			}
//		}, new Integer(1)).get();
//		if (!treeSum.equals(refSum))
//			throw new RuntimeException("wrong inject result! expected " + refSum + " but got " + treeSum);
//		dot();
//
//		d.exec(t.getIdentifier(), "each", new DMap.DefaultDEach<String, String>() {
//			public void each(Map.Entry<String, String> entry) {
//				if (entry.getKey().equals(middleKey)) {
//					remove();
//				}
//			}
//		}).get();
//		ref.remove(middleKey);
//		dot();
//
//		verifyTreap(d, t, ref);
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static List<DMap.DMapEntry<String, String>> getNodes(Dhasher d, DTreap treap) {
//		Delay<List<DMap.DMapEntry<String, String>>> nodesDelay = d.exec(treap.getIdentifier(), "inject",
//				new ArrayList<DMap.DMapEntry<String, String>>(), new DMap.DefaultDInject<List<DMap.DMapEntry<String, String>>, String, String>() {
//					public List<DMap.DMapEntry<String, String>> inject(List<DMap.DMapEntry<String, String>> sum, Map.Entry<String, String> e) {
//						sum.add(getEntry());
//						return sum;
//					}
//				});
//		return nodesDelay.get();
//	}
//
//	public static void testRemove() throws Exception {
//		System.out.print("\tTesting DTreap removal");
//
//		setEnableSynchronize(false);
//
//		destroy();
//		ensureEmpty();
//		dot();
//
//		Dhasher d = new Dhasher(dhash4);
//		dot();
//		DTreap t = new DTreap();
//		d.put(t).get();
//		dot();
//		SortedMap<String, String> ref = new TreeMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			String k = "" + Math.random();
//			String v = "" + Math.random();
//			if (d.exec(t.getIdentifier(), "put", k, v).get() != null)
//				throw new RuntimeException("put returned non null");
//			ref.put(k, v);
//			dot();
//		}
//
//		verifyTreap(d, t, ref);
//
//		Collection<DMap.DMapEntry<String, String>> nodes = getNodes(d, t);
//		dot();
//
//		d.exec(t.getIdentifier(), "remove").get();
//
//		for (DMap.DMapEntry<String, String> node : nodes) {
//			if (d.get(node.getIdentifier()).get() != null)
//				throw new RuntimeException("entry not removed!");
//			dot();
//		}
//
//		if (d.get(t.getIdentifier()).get() != null)
//			throw new RuntimeException("remove failed!");
//		dot();
//
//		ensureEmpty();
//
//		setEnableSynchronize(true);
//		System.out.println("done!");
//	}
//
//	public static void testClear() throws Exception {
//		System.out.print("\tTesting DTreap clearing");
//
//		Dhasher d = new Dhasher(dhash4);
//		dot();
//		DTreap t = new DTreap();
//		d.put(t).get();
//		dot();
//		SortedMap<String, String> ref = new TreeMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			String k = "" + Math.random();
//			String v = "" + Math.random();
//			if (d.exec(t.getIdentifier(), "put", k, v).get() != null)
//				throw new RuntimeException("put returned non null");
//			ref.put(k, v);
//			dot();
//		}
//
//		verifyTreap(d, t, ref);
//
//		Collection<DMap.DMapEntry<String, String>> nodes = getNodes(d, t);
//		dot();
//
//		d.exec(t.getIdentifier(), "each", new DMap.DefaultDEach<String, String>() {
//			public void each(Map.Entry<String, String> entry) {
//				remove();
//				dot();
//			}
//		}).get();
//		ref.clear();
//
//		verifyTreap(d, t, ref);
//
//		for (DMap.DMapEntry<String, String> node : nodes) {
//			if (d.get(node.getIdentifier()).get() != null)
//				throw new RuntimeException("entry not removed!");
//			dot();
//		}
//
//		ref = new TreeMap<String, String>();
//		for (int i = 0; i < 10; i++) {
//			String k = "" + Math.random();
//			String v = "" + Math.random();
//			if (d.exec(t.getIdentifier(), "put", k, v).get() != null)
//				throw new RuntimeException("put returned non null");
//			ref.put(k, v);
//			dot();
//		}
//
//		verifyTreap(d, t, ref);
//		dot();
//
//		nodes = getNodes(d, t);
//
//		dot();
//
//		d.exec(t.getIdentifier(), "clear").get();
//
//		ref.clear();
//
//		verifyTreap(d, t, ref);
//		dot();
//
//		for (DMap.DMapEntry<String, String> node : nodes) {
//			if (d.get(node.getIdentifier()).get() != null)
//				throw new RuntimeException("entry not removed!");
//			dot();
//		}
//
//		System.out.println("done!");
//
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		System.out.println("Testing DTreap");
//		testInject();
//		testPutGetAtDel();
//		testClear();
//		testRemove();
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		test(arguments);
//		teardown();
//	}
//
//}