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
//import java.net.InetSocketAddress;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.Callable;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import cx.ath.troja.chordless.ServerInfo;
//import cx.ath.troja.chordless.commands.Command;
//import cx.ath.troja.chordless.commands.PingCommand;
//import cx.ath.troja.chordless.dhash.DHash;
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.Entry;
//import cx.ath.troja.chordless.dhash.ExecDhasher;
//import cx.ath.troja.chordless.dhash.Persistent;
//import cx.ath.troja.chordless.dhash.Persister;
//import cx.ath.troja.chordless.dhash.Receiver;
//import cx.ath.troja.chordless.dhash.RemoteDhasher;
//import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
//import cx.ath.troja.chordless.dhash.storage.LockingStorage;
//import cx.ath.troja.chordless.dhash.structures.DCollection;
//import cx.ath.troja.chordless.dhash.structures.DList;
//import cx.ath.troja.chordless.dhash.structures.DMap;
//import cx.ath.troja.chordless.dhash.transactions.Transaction;
//import cx.ath.troja.nja.FutureValue;
//import cx.ath.troja.nja.Identifier;
//import cx.ath.troja.nja.JDBC;
//
//public class TestDHash extends TestAll {
//
//	public static DHash[] dhashes;
//
//	public static DHash dhash1, dhash2, dhash3, dhash4, dhash5;
//
//	public static class TimerClock {
//		long sum = 0;
//
//		long iterations = 0;
//
//		public void start() {
//			sum -= System.currentTimeMillis();
//		}
//
//		public void stop() {
//			sum += System.currentTimeMillis();
//			iterations++;
//		}
//
//		public long avg() {
//			return sum / iterations;
//		}
//	}
//
//	public static class TestLink extends Persistent {
//		private String id;
//
//		private String next;
//
//		private String blurb;
//
//		public TestLink(String i, String n) {
//			id = i;
//			next = n;
//			blurb = "blurb";
//		}
//
//		public void setBlurb(String s) {
//			blurb = s;
//		}
//
//		public String getBlurb() {
//			return blurb;
//		}
//
//		public String getNext() {
//			return next;
//		}
//
//		public Integer length() {
//			return id.length();
//		}
//
//		public Object getId() {
//			return id;
//		}
//	}
//
//	public static void testEnvoys(Dhasher d) throws Exception {
//		System.out.print("\tTesting envoy commands");
//		d.put(new TestLink("gunde", null)).get();
//		dot();
//		d.put(new TestLink("berta", "gunde")).get();
//		dot();
//		Delay<Integer> delay = d.envoy(new Persister.Envoy<TestLink>() {
//			private Integer sum = 0;
//
//			public void handle(TestLink t) {
//				sum += t.length();
//				if (t.getNext() == null) {
//					returnHome(sum);
//				} else {
//					redirect(t.getNext());
//				}
//			}
//		}, "berta");
//		dot();
//		if (delay.get().intValue() != ("gunde".length() + "berta".length()))
//			throw new RuntimeException("envoys failed!");
//		dot();
//
//		Transaction t = d.transaction();
//		dot();
//
//		delay = t.envoy(new Persister.Envoy<TestLink>() {
//			private Integer sum = 0;
//
//			public void handle(TestLink t) {
//				sum += t.length();
//				if (t.getNext() == null) {
//					returnHome(sum);
//				} else {
//					redirect(t.getNext());
//				}
//			}
//		}, "berta");
//		dot();
//		if (delay.get().intValue() != ("gunde".length() + "berta".length()))
//			throw new RuntimeException("envoys failed!");
//		dot();
//
//		if (!d.exec("berta", "getBlurb").get().equals("blurb"))
//			throw new RuntimeException("bad blurb");
//		if (!d.exec("gunde", "getBlurb").get().equals("blurb"))
//			throw new RuntimeException("bad blurb");
//		delay = d.envoy(new Persister.Envoy<TestLink>() {
//			public void handle(TestLink t) {
//				t.setBlurb("new blurb");
//				if (t.getNext() == null) {
//					returnHome(null);
//				} else {
//					redirect(t.getNext());
//				}
//			}
//		}, "berta");
//		dot();
//		delay.get();
//		if (!d.exec("berta", "getBlurb").get().equals("new blurb"))
//			throw new RuntimeException("envoy did not modify");
//		if (!d.exec("gunde", "getBlurb").get().equals("new blurb"))
//			throw new RuntimeException("envoy did not modify");
//		dot();
//
//		t = d.transaction();
//		dot();
//		delay = t.envoy(new Persister.Envoy<TestLink>() {
//			public void handle(TestLink t) {
//				t.setBlurb("newer blurb");
//				if (t.getNext() == null) {
//					returnHome(null);
//				} else {
//					redirect(t.getNext());
//				}
//			}
//		}, "berta");
//		dot();
//		delay.get();
//		t.commit();
//		dot();
//		if (!d.exec("berta", "getBlurb").get().equals("newer blurb"))
//			throw new RuntimeException("envoy did not modify");
//		if (!d.exec("gunde", "getBlurb").get().equals("newer blurb"))
//			throw new RuntimeException("envoy did not modify");
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static class TestData extends Persistent {
//		private String name;
//
//		private String epa;
//
//		public TestData(String n) {
//			name = n;
//			epa = n;
//		}
//
//		public Object getId() {
//			return name;
//		}
//
//		public void setEpa(String n) {
//			epa = n;
//		}
//
//		public String delayedHello(Long wait) {
//			try {
//				Thread.sleep(wait);
//			} catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//			return "hello";
//		}
//
//		public String delayedHello(Identifier i, Long wait1, Long wait2) {
//			return (String) exec(i, "delayedHello", wait1).get(wait2.longValue());
//		}
//
//		public String getEpa() {
//			return epa;
//		}
//
//		public void setEpaOfBoth(String key, final String newEpa) {
//			exec(Identifier.generate(key), "setEpa", newEpa).get();
//			setEpa(newEpa);
//		}
//
//		public Integer lengthOf(String key) {
//			Delay<Integer> f = exec(Identifier.generate(key), "length");
//			return f.get();
//		}
//
//		public int lengthOf(String[] keys) {
//			Collection<Delay<Integer>> futures = new LinkedList<Delay<Integer>>();
//			for (int i = 0; i < keys.length; i++) {
//				Delay<Integer> f = exec(Identifier.generate(keys[i]), "length");
//				futures.add(f);
//			}
//			int sum = 0;
//			for (Delay<Integer> future : futures) {
//				sum = sum + future.get();
//			}
//			return sum;
//		}
//	}
//
//	public static String randomValue() {
//		StringBuffer rval = new StringBuffer();
//		for (int i = 0; i < (int) (Math.random() * 4096); i++) {
//			rval.append("" + ((int) (Math.random() * 10)));
//		}
//
//		return rval.toString();
//	}
//
//	public static Map<Object, Object> getContent(Dhasher d, DMap map) {
//		Delay<Map<Object, Object>> delay = d.exec(map.getIdentifier(), "inject", new HashMap<Object, Object>(),
//				new DMap.DefaultDInject<Map<Object, Object>, String, String>() {
//					public Map<Object, Object> inject(Map<Object, Object> sum, Map.Entry<String, String> entry) {
//						sum.put(entry.getKey(), entry.getValue());
//						return sum;
//					}
//				});
//		return delay.get();
//	}
//
//	public static List<Object> getContent(Dhasher d, DCollection list) {
//		Delay<List<Object>> delay = d.exec(list.getIdentifier(), "inject", new ArrayList<Object>(),
//				new DCollection.DefaultDInject<List<Object>, String>() {
//					public List<Object> inject(List<Object> sum, String s) {
//						sum.add(s);
//						return sum;
//					}
//				});
//		return delay.get();
//	}
//
//	public static List<DList.Element<String>> getElements(Dhasher d, DCollection<String> list) {
//		Delay<List<DList.Element<String>>> elementDelay = d.exec(list.getIdentifier(), "inject", new ArrayList<DList.Element<String>>(),
//				new DList.Inject<List<DList.Element<String>>, String>() {
//					public List<DList.Element<String>> inject(List<DList.Element<String>> sum, String s) {
//						sum.add(getElement());
//						return sum;
//					}
//				});
//		return elementDelay.get();
//	}
//
//	public static void testPutGetExecDel(Dhasher dhasher) throws Exception {
//		TimerClock gets = new TimerClock();
//		TimerClock puts = new TimerClock();
//		TimerClock execs = new TimerClock();
//		TimerClock dels = new TimerClock();
//		System.out.print("\tTesting DHash put/get/exec/del");
//		for (int i = 0; i < 20; i++) {
//			String thisKey = "test" + i;
//			String thisValue = randomValue();
//			puts.start();
//			dhasher.put(thisKey, thisValue).get();
//			puts.stop();
//			gets.start();
//			Object what = dhasher.get(thisKey).get();
//			gets.stop();
//			if (!thisValue.equals(what)) {
//				throw new RuntimeException("Bad get! dhasher.get(" + thisKey + ") should be " + thisValue + " but is " + what + "!");
//			}
//			execs.start();
//			if (!dhasher.exec(thisKey, "toString", new Object[0]).get().equals(thisValue.toString()))
//				throw new RuntimeException("Bad exec!");
//			execs.stop();
//			if (!dhasher.commute(thisKey, "toString", new Object[0]).get().equals(thisValue.toString()))
//				throw new RuntimeException("bad commute!");
//			dels.start();
//			dhasher.del(thisKey).get();
//			dels.stop();
//			Object o = dhasher.get(thisKey).get();
//			if (o != null)
//				throw new RuntimeException("Del didnt work! i got " + o);
//			dot();
//		}
//		System.out.println("done!");
//		System.out.println("\t\tavg put: " + puts.avg() + "ms, avg get: " + gets.avg() + "ms, avg exec: " + execs.avg() + "ms, avg del: "
//				+ dels.avg() + "ms");
//	}
//
//	public static void testGlobalCleaning(final DHash dhash, Dhasher d) throws Exception {
//		System.out.print("\tTesting DHash global cleanup");
//		del(dhash, dhash.getIdentifier().next());
//		dot();
//		put(dhash, new Entry(dhash.getIdentifier().next(), "prul"));
//		if (dhash.getPersistExecutor().submit(new Callable<Entry>() {
//			public Entry call() {
//				return dhash.getStorage().getEmpty(dhash.getIdentifier().next());
//			}
//		}).get() == null)
//			throw new RuntimeException("writing failed");
//		dhash.resetLastCleanCommand();
//		clean(dhash);
//		dot();
//		for (int i = 0; i < 500; i++) {
//			if (dhash.getPersistExecutor().submit(new Callable<Entry>() {
//				public Entry call() {
//					return dhash.getStorage().getEmpty(dhash.getIdentifier().next());
//				}
//			}).get() == null)
//				break;
//			Thread.sleep(100);
//			dot();
//		}
//		if (dhash.getPersistExecutor().submit(new Callable<Entry>() {
//			public Entry call() {
//				return dhash.getStorage().getEmpty(dhash.getIdentifier().next());
//			}
//		}).get() != null)
//			throw new RuntimeException("cleanup failed");
//		dot();
//		if (!d.get(dhash.getIdentifier().next()).get().equals("prul"))
//			throw new RuntimeException("moving of cleaned up data failed!");
//		dot();
//		System.out.println("done!");
//	}
//
//	public static Entry getEmpty(final DHash dhash, final Identifier i) throws Exception {
//		return dhash.getPersistExecutor().submit(new Callable<Entry>() {
//			public Entry call() {
//				return dhash.getStorage().getEmpty(i);
//			}
//		}).get();
//	}
//
//	public static Entry get(final DHash dhash, final Identifier i) throws Exception {
//		return dhash.getPersistExecutor().submit(new Callable<Entry>() {
//			public Entry call() {
//				return dhash.getStorage().get(i);
//			}
//		}).get();
//	}
//
//	public static void testRedundancy(Dhasher d) throws Exception {
//		setEnableSynchronize(false);
//		System.out.print("\tTesting redundancy");
//		d.del("buffel").get();
//		dot();
//		d.put("buffel", "balthazar").get();
//		dot();
//		List<DHash> containers = new LinkedList<DHash>();
//		for (int j = 0; j < 20; j++) {
//			containers = new LinkedList<DHash>();
//			for (int i = 0; i < dhashes.length; i++) {
//				final int x = i;
//				if (getEmpty(dhashes[i], Identifier.generate("buffel")) != null) {
//					containers.add(dhashes[i]);
//				}
//				dot();
//			}
//			if (containers.size() == dhashes[0].getCopies())
//				break;
//			else
//				Thread.sleep(100);
//		}
//		if (containers.size() != dhashes[0].getCopies())
//			throw new RuntimeException("wrong number of copies out there! is " + containers.size() + " but should be " + dhashes[0].getCopies());
//		dot();
//		for (int i = 0; i < containers.size(); i++) {
//			for (int j = 0; j < containers.size(); j++) {
//				if (j != i) {
//					del(containers.get(j), Identifier.generate("buffel"));
//				}
//			}
//			if (d.get("buffel").get() == null)
//				throw new RuntimeException("unable to fetch with only one copy!");
//			if (!d.exec("buffel", "length", new Object[0]).get().equals(new Integer("balthazar".length())))
//				throw new RuntimeException("unable to exec with only one copy!");
//			for (int j = 0; j < containers.size(); j++) {
//				if (j != i) {
//					put(containers.get(j), new Entry(Identifier.generate("buffel"), "balthazar"));
//				}
//			}
//			dot();
//		}
//		d.del("buffel").get();
//		Object o = d.get("buffel").get();
//		if (o != null)
//			throw new RuntimeException("unable to delete properly! got " + o);
//		setEnableSynchronize(true);
//		System.out.println("done!");
//	}
//
//	public static DHash find(Identifier i) {
//		for (int j = 0; j < dhashes.length; j++) {
//			if (dhashes[j].getIdentifier().equals(i)) {
//				return dhashes[j];
//			}
//		}
//		return null;
//	}
//
//	public static boolean doneJoining() {
//		for (int i = 0; i < dhashes.length; i++) {
//			if (dhashes[i].getPredecessor() == null) {
//				return false;
//			}
//			ServerInfo[] successors = dhashes[i].getSuccessorArray();
//			if (successors.length < dhashes.length + 1) {
//				return false;
//			}
//			for (int j = 0; j < dhashes.length; j++) {
//				if (successors[j] == null) {
//					return false;
//				}
//				int dhashIndex = (i + j + 1) % dhashes.length;
//				if (!successors[j].getIdentifier().equals(dhashes[dhashIndex].getIdentifier())) {
//					return false;
//				}
//			}
//		}
//		return true;
//	}
//
//	public static void testJoin() throws Exception {
//		System.out.print("Testing join");
//		while (!doneJoining()) {
//			Thread.sleep(100);
//			dot();
//		}
//		System.out.println("done!");
//	}
//
//	public static void testModifyingExec(DHash dhash, Dhasher d) throws Exception {
//		System.out.print("\tTesting modifying executions");
//		d.put(new TestData("klur")).get();
//		Thread.sleep(100);
//		int holders = 0;
//		for (int i = 0; i < dhashes.length; i++) {
//			Entry e = getEmpty(dhashes[i], Identifier.generate("klur"));
//			if (e != null) {
//				holders++;
//				e = get(dhashes[i], Identifier.generate("klur"));
//				if (!((TestData) e.getValue()).getEpa().equals("klur"))
//					throw new RuntimeException("wrong data in backup db!");
//			}
//		}
//		dot();
//		if (holders != dhash.getCopies()) {
//			throw new RuntimeException("wrong number of holders! is " + holders + " should be " + dhash.getCopies() + "!");
//		}
//		dot();
//		if (!d.exec("klur", "getEpa").get().equals("klur"))
//			throw new RuntimeException("wrong data in db! wanted klur but got " + d.exec("klur", "getEpa").get());
//		dot();
//		d.exec("klur", "setEpa", "klapp").get();
//		Thread.sleep(100);
//		if (!d.exec("klur", "getEpa").get().equals("klapp"))
//			throw new RuntimeException("wrong data in db!");
//		dot();
//		holders = 0;
//		for (int i = 0; i < dhashes.length; i++) {
//			Entry e = getEmpty(dhashes[i], Identifier.generate("klur"));
//			if (e != null) {
//				holders++;
//				e = get(dhashes[i], Identifier.generate("klur"));
//				if (!((TestData) e.getValue()).getEpa().equals("klapp"))
//					throw new RuntimeException("wrong data in backup db!");
//			}
//		}
//		dot();
//		if (holders != dhash.getCopies()) {
//			throw new RuntimeException("wrong number of holders! is " + holders + " should be " + dhash.getCopies() + "!");
//		}
//
//		d.put(new TestData("bappa")).get();
//		dot();
//		if (!d.exec("bappa", "getEpa").get().equals("bappa"))
//			throw new RuntimeException("wrong data in db!");
//		dot();
//		d.exec("klur", "setEpaOfBoth", "bappa", "ojsan").get();
//		dot();
//		if (!d.exec("bappa", "getEpa").get().equals("ojsan"))
//			throw new RuntimeException("wrong data in db!");
//		dot();
//		if (!d.exec("klur", "getEpa").get().equals("ojsan"))
//			throw new RuntimeException("wrong data in db!");
//		System.out.println("done!");
//	}
//
//	public static void testChainedExecs(Dhasher d) {
//		System.out.print("\tTesting chained executions");
//		d.put("pluring", "bamse").get();
//		dot();
//		d.put("tjafs", new TestData("tjafs")).get();
//		dot();
//		if (!d.exec("tjafs", "lengthOf", "pluring").get().equals(new Integer("bamse".length())))
//			throw new RuntimeException("couldnt chain calls!");
//		dot();
//		d.put("plopp", "gneg").get();
//		dot();
//		Object response = d.exec("tjafs", "lengthOf", new Object[] { new String[] { "pluring", "plopp" } }).get();
//		if (response instanceof Exception) {
//			((Exception) response).printStackTrace(System.out);
//		}
//		if (!response.equals(new Integer("bamsegneg".length())))
//			throw new RuntimeException("couldnt inject calls! wanted " + "bamegneg".length() + " but got " + response);
//		System.out.println("done!");
//	}
//
//	public static void setEnableSynchronize(boolean b) {
//		for (int i = 0; i < dhashes.length; i++) {
//			dhashes[i].setEnableSynchronize(b);
//		}
//	}
//
//	public static void setEnableClean(boolean b) {
//		for (int i = 0; i < dhashes.length; i++) {
//			dhashes[i].setEnableClean(b);
//		}
//	}
//
//	public static void testDistribution(int owned) throws Exception {
//		for (int i = 0; i < dhashes.length; i++) {
//			final int x = i;
//			int held = dhashes[i].getPersistExecutor().submit(new Callable<Integer>() {
//				public Integer call() {
//					return dhashes[x].getEntriesHeld();
//				}
//			}).get();
//			if (held != owned * dhashes[0].getCopies())
//				throw new RuntimeException("expected to hold " + (owned * dhashes[0].getCopies()) + " but held " + held);
//			int o = dhashes[i].getPersistExecutor().submit(new Callable<Integer>() {
//				public Integer call() {
//					return dhashes[x].getEntriesOwned();
//				}
//			}).get();
//			if (o != owned)
//				throw new RuntimeException("expected to own " + owned + " but owned " + o);
//		}
//	}
//
//	public static void destroy(boolean dots) throws Exception {
//		for (int i = 0; i < dhashes.length; i++) {
//			destroy(dhashes[i]);
//			if (dots)
//				dot();
//		}
//		ensureEmpty();
//	}
//
//	public static void ensureEmpty() throws Exception {
//		Map<Identifier, Entry> nonEmpty = null;
//		for (int j = 0; j < 20; j++) {
//			nonEmpty = new HashMap<Identifier, Entry>();
//			for (int i = 0; i < dhashes.length; i++) {
//				Map<Identifier, Entry> m = getEmpty(dhashes[i]);
//				for (Map.Entry<Identifier, Entry> entry : m.entrySet()) {
//					Entry previous = nonEmpty.get(entry.getKey());
//					if (previous == null) {
//						nonEmpty.put(entry.getValue().getIdentifier(), entry.getValue());
//					} else if (previous.getTimestamp() < entry.getValue().getTimestamp()) {
//						nonEmpty.put(entry.getValue().getIdentifier(), entry.getValue());
//					}
//				}
//			}
//			Iterator<Map.Entry<Identifier, Entry>> nonEmptyIter = nonEmpty.entrySet().iterator();
//			while (nonEmptyIter.hasNext()) {
//				if (nonEmptyIter.next().getValue().getValueClassName().equals("null")) {
//					nonEmptyIter.remove();
//				}
//			}
//			if (nonEmpty.size() == 0) {
//				break;
//			} else {
//				Thread.sleep(100);
//			}
//		}
//		if (nonEmpty.size() > 0) {
//			StringBuffer left = new StringBuffer();
//			for (Map.Entry<Identifier, Entry> entry : nonEmpty.entrySet()) {
//				left.append("\n" + entry.getValue());
//			}
//			throw new RuntimeException("db should be empty! but found " + left);
//		}
//	}
//
//	public static void destroy() throws Exception {
//		destroy(true);
//	}
//
//	public static void testSynchronize(Dhasher d) throws Exception {
//		System.out.print("\tTesting synchronize");
//		setEnableClean(false);
//		setEnableSynchronize(false);
//		destroy();
//		dot();
//		int x = 100;
//		int num = x / dhashes.length;
//		for (int j = 0; j < dhashes.length; j++) {
//			final int dhashno = j;
//			Identifier next = dhashes[dhashno].getIdentifier();
//			for (int i = 0; i < num; i++) {
//				next = next.previous();
//				final Identifier thisId = next;
//				put(dhashes[dhashno], new Entry(thisId, "kompis"));
//			}
//		}
//
//		setEnableSynchronize(true);
//
//		for (int j = 0; j < x * 3; j++) {
//			try {
//				testDistribution(num);
//				break;
//			} catch (Exception e) {
//				Thread.sleep(100);
//				dot();
//			}
//		}
//		testDistribution(num);
//		setEnableClean(true);
//		System.out.println("done!");
//	}
//
//	public static Collection<Entry> produceEntries(int n, Identifier last) {
//		Collection<Entry> returnValue = new LinkedList<Entry>();
//		for (int i = 0; i < n; i++) {
//			last = last.previous();
//			returnValue.add(new Entry(last, "blammo"));
//		}
//		return returnValue;
//	}
//
//	public static int entriesHeld(final DHash dhash) throws Exception {
//		return dhash.getPersistExecutor().submit(new Callable<Integer>() {
//			public Integer call() {
//				return dhash.getEntriesHeld();
//			}
//		}).get().intValue();
//	}
//
//	public static int entriesOwned(final DHash dhash) throws Exception {
//		return dhash.getPersistExecutor().submit(new Callable<Integer>() {
//			public Integer call() {
//				return dhash.getEntriesOwned();
//			}
//		}).get().intValue();
//	}
//
//	private static void clean(final DHash dhash) throws Exception {
//		final FutureValue<Object> f = new FutureValue<Object>();
//		dhash.getPersistExecutor().execute(new Runnable() {
//			public void run() {
//				dhash._resetCleanTimestamp();
//				dhash.clean();
//				f.set("ff");
//			}
//		});
//		f.get();
//	}
//
//	public static void testCleanAndSynchronize() throws Exception {
//		System.out.println("Testing numerical consistency of clean and synchronize");
//		setEnableClean(false);
//		setEnableSynchronize(false);
//
//		System.out.print("\tFilling data");
//		for (int i = 0; i < dhashes.length; i++) {
//			destroy(dhashes[i]);
//			dhashes[i].setCopies(1);
//			dot();
//		}
//
//		int n = 50;
//
//		for (int i = 0; i < dhashes.length; i++) {
//			Identifier id = dhashes[i].getIdentifier().previous();
//			put(dhashes[i], produceEntries(n, id));
//			dot();
//		}
//
//		for (int i = 0; i < dhashes.length; i++) {
//			if (entriesHeld(dhashes[i]) != n)
//				throw new RuntimeException("wrong number of entries held!");
//			if (entriesOwned(dhashes[i]) != n)
//				throw new RuntimeException("wrong number of entries held!");
//			dot();
//		}
//		System.out.println("done!");
//
//		System.out.print("\tSynchronizing data");
//		setEnableSynchronize(true);
//		for (int i = 0; i < dhashes.length; i++) {
//			dhashes[i].setCopies(2);
//			dot();
//		}
//
//		for (int i = 0; i < dhashes.length; i++) {
//			final FutureValue<Object> f = new FutureValue<Object>();
//			final int finalI = i;
//			dhashes[i].getPersistExecutor().execute(new Runnable() {
//				public void run() {
//					dhashes[finalI].synchronize();
//					f.set("ff");
//				}
//			});
//			f.get();
//			dot();
//		}
//
//		for (int j = 0; j < 200; j++) {
//			int done = 0;
//			for (int i = 0; i < dhashes.length; i++) {
//				if (entriesHeld(dhashes[i]) == n * 2 && entriesOwned(dhashes[i]) == n)
//					done++;
//			}
//			if (done == dhashes.length)
//				break;
//			Thread.sleep(100);
//			dot();
//		}
//
//		for (int i = 0; i < dhashes.length; i++) {
//			if (entriesHeld(dhashes[i]) != n * 2)
//				throw new RuntimeException("wrong number of entries held! is " + entriesHeld(dhashes[i]) + " but should be " + (n * 2));
//			if (entriesOwned(dhashes[i]) != n)
//				throw new RuntimeException("wrong number of entries held! is " + entriesOwned(dhashes[i]) + " but should be " + n);
//			dot();
//		}
//		System.out.println("done!");
//
//		System.out.print("\tCleaning data");
//		for (int i = 0; i < dhashes.length; i++) {
//			dhashes[i].setCopies(2);
//		}
//
//		long[] timestamps = new long[dhashes.length];
//
//		setEnableClean(true);
//		for (int i = 0; i < dhashes.length; i++) {
//			clean(dhashes[i]);
//			dot();
//		}
//
//		for (int j = 0; j < 100; j++) {
//			boolean ok = true;
//			for (int i = 0; i < dhashes.length; i++) {
//				if (entriesHeld(dhashes[i]) != n * 2)
//					ok = false;
//				if (entriesOwned(dhashes[i]) != n)
//					ok = false;
//			}
//			if (ok)
//				break;
//			Thread.sleep(100);
//			dot();
//		}
//
//		for (int i = 0; i < dhashes.length; i++) {
//			if (entriesHeld(dhashes[i]) != n * 2)
//				throw new RuntimeException("wrong number of entries held! is " + entriesHeld(dhashes[i]) + " but should be " + (n * 2));
//			if (entriesOwned(dhashes[i]) != n)
//				throw new RuntimeException("wrong number of entries held! is " + entriesOwned(dhashes[i]) + " but should be " + n);
//			dot();
//		}
//
//		System.out.println("done!");
//
//		System.out.print("\tCleaning data again");
//		for (int i = 0; i < dhashes.length; i++) {
//			dhashes[i].setCopies(1);
//		}
//
//		for (int i = 0; i < dhashes.length; i++) {
//			clean(dhashes[i]);
//			dot();
//		}
//
//		for (int j = 0; j < 400; j++) {
//			boolean ok = true;
//			for (int i = 0; i < dhashes.length; i++) {
//				if (entriesHeld(dhashes[i]) != n)
//					ok = false;
//				if (entriesOwned(dhashes[i]) != n)
//					ok = false;
//			}
//			dot();
//			if (ok)
//				break;
//			Thread.sleep(100);
//		}
//
//		for (int i = 0; i < dhashes.length; i++) {
//			if (entriesHeld(dhashes[i]) != n)
//				throw new RuntimeException("wanted to hold " + n + " but held " + entriesHeld(dhashes[i]));
//			if (entriesOwned(dhashes[i]) != n)
//				throw new RuntimeException("wanted to own " + n + " but owned " + entriesOwned(dhashes[i]));
//		}
//		dot();
//
//		for (int i = 0; i < dhashes.length; i++) {
//			destroy(dhashes[i]);
//			dhashes[i].setCopies(3);
//			dot();
//		}
//
//		setEnableSynchronize(true);
//		setEnableClean(true);
//		System.out.println("done!");
//	}
//
//	public static void destroy(final DHash dhash) throws Exception {
//		dhash.getPersistExecutor().submit(new Runnable() {
//			public void run() {
//				dhash.getStorage().destroy();
//			}
//		}).get();
//	}
//
//	public static void testRemove(Dhasher d) throws Exception {
//		System.out.print("\tTesting removal of Persistent instances");
//		TestData t = new TestData("mababa");
//		dot();
//		d.put(t).get();
//		dot();
//		if (!((TestData) d.get("mababa").get()).getId().equals(t.getId()))
//			throw new RuntimeException("failed writing");
//		dot();
//		d.exec("mababa", "remove").get();
//		dot();
//		if (d.get("mababa").get() != null)
//			throw new RuntimeException("failed removal");
//		System.out.println("done!");
//	}
//
//	public static void testLocking(final Identifier i) throws Exception {
//		Identifier lockSource = Identifier.random();
//
//		ExecDhasher d = new ExecDhasher(dhashes[0], dhashes[0].getServerInfo());
//
//		if (((LockingStorage.LockResponse) d.prepare(i, d.getVersion(i).get(), d.getCommutation(i).get(), lockSource).get()).code != LockingStorage.LockResponse.LOCK_SUCCESS)
//			throw new RuntimeException("locking failed!");
//		dot();
//
//		Identifier randomIdent = Identifier.random();
//
//		d.put(i, randomIdent);
//		dot();
//		Thread.sleep(200);
//
//		if (randomIdent.equals(d.get(i).get()))
//			throw new RuntimeException("locking failed!");
//		dot();
//
//		Identifier otherLockSource = Identifier.random();
//
//		Delay<LockingStorage.LockResponse> otherLock = d.prepare(i, d.getVersion(i).get(), d.getCommutation(i).get(), otherLockSource);
//
//		if (((LockingStorage.LockResponse) d.prepare(i, d.getVersion(i).get(), d.getCommutation(i).get(), lockSource).get()).code != LockingStorage.LockResponse.LOCK_SUCCESS)
//			throw new RuntimeException("samelock should work!");
//
//		d.abort(i, lockSource).get();
//		dot();
//		Thread.sleep(200);
//
//		if (!randomIdent.equals(d.get(i).get()))
//			throw new RuntimeException("unlocking failed!");
//		dot();
//
//		if (otherLock.get().code == LockingStorage.LockResponse.LOCK_SUCCESS)
//			throw new RuntimeException("locking should not have succeeded");
//		dot();
//
//	}
//
//	public static void testLocking(Dhasher d) throws Exception {
//		setEnableClean(false);
//		setEnableSynchronize(false);
//		System.out.println("\tTesting locking");
//		System.out.print("\t\tTesting locking on existing entries");
//		dot();
//		final Identifier i = dhashes[0].getIdentifier().previous();
//		dot();
//		d.put(i, "blapp").get();
//		dot();
//		if (!d.get(i).get().equals("blapp"))
//			throw new RuntimeException("writing failed");
//		dot();
//		d.put(i, "blipp").get();
//		dot();
//		if (!d.get(i).get().equals("blipp"))
//			throw new RuntimeException("writing failed");
//		dot();
//		testLocking(i);
//		System.out.println("done!");
//		System.out.print("\t\tTesting locking on non-existing entries");
//		testLocking(Identifier.random());
//		System.out.println("done!");
//		setEnableClean(true);
//		setEnableSynchronize(true);
//	}
//
//	public static void testTimeouts(Dhasher d) throws Exception {
//		System.out.print("\tTesting timeouts");
//		dot();
//		d.put("blupp", new TestData("blupp")).get();
//		dot();
//		d.put("blupp2", new TestData("blupp2")).get();
//		dot();
//		if (!d.exec("blupp", "delayedHello", new Long(0)).get().equals("hello"))
//			throw new RuntimeException("exec failed!");
//		dot();
//		try {
//			d.exec("blupp", "delayedHello", new Long(1000)).get(100);
//			throw new RuntimeException("no timeout exception thrown!");
//		} catch (Delay.TimeoutException e) {
//			dot();
//		}
//		try {
//			d.exec("blupp", "delayedHello", Identifier.generate("blupp2"), new Long(1000), new Long(100)).get();
//			throw new RuntimeException("no timeout exception thrown!");
//		} catch (Delay.TimeoutException e) {
//			dot();
//		}
//		System.out.println("done!");
//	}
//
//	public static void testGC() throws Exception {
//		System.out.print("Testing garbage collect");
//		destroy();
//		dhash1.setEnableGC(false);
//		dhash1.setEnableClean(false);
//		dhash1.setEnableSynchronize(false);
//		dot();
//		final LinkedBlockingQueue<Object> q = new LinkedBlockingQueue<Object>();
//		Identifier i = dhash1.getIdentifier().next();
//		final Identifier x = i;
//		dhash1.getPersistExecutor().submit(new Runnable() {
//			public void run() {
//				dhash1.getStorage().put(new Entry(x, "plupp", System.currentTimeMillis(), 0, 0, 0, "String", null), null,
//						new LockingStorage.Returner<Object>() {
//							public void call(Object b) {
//								q.add(new Object());
//							}
//						});
//			}
//		}).get();
//
//		dot();
//		q.take();
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("writing failed!");
//
//		dot();
//		dhash1.resetGC();
//
//		dhash1.setEnableGC(true);
//		dhash1.gc();
//		while (dhash1.runningGC()) {
//			Thread.sleep(100);
//		}
//		dhash1.setEnableGC(false);
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("gc cleaned new non null entries!");
//		dot();
//		i = i.next();
//		final Identifier y = i;
//		dhash1.getPersistExecutor().submit(new Runnable() {
//			public void run() {
//				dhash1.getStorage().put(new Entry(y, "plupp", 0, 0, 0, 0, "String", null), null, new LockingStorage.Returner<Object>() {
//					public void call(Object b) {
//						q.add(new Object());
//					}
//				});
//			}
//		}).get();
//		dot();
//		q.take();
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("writing failed!");
//
//		dhash1.resetGC();
//
//		dhash1.setEnableGC(true);
//		dhash1.gc();
//		while (dhash1.runningGC()) {
//			Thread.sleep(100);
//		}
//		dhash1.setEnableGC(false);
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("gc cleaned old non null entries!");
//		dot();
//		i = i.next();
//		final Identifier z = i;
//		dhash1.getPersistExecutor().submit(new Runnable() {
//			public void run() {
//				dhash1.getStorage().put(new Entry(z, "plupp", System.currentTimeMillis(), 0, 0, 0, "null", null), null,
//						new LockingStorage.Returner<Object>() {
//							public void call(Object b) {
//								q.add(new Object());
//							}
//						});
//			}
//		}).get();
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("writing failed!");
//
//		dot();
//		q.take();
//		dhash1.resetGC();
//		dhash1.setEnableGC(true);
//		dhash1.gc();
//		while (dhash1.runningGC()) {
//			Thread.sleep(100);
//		}
//		dhash1.setEnableGC(false);
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("gc cleaned new null entries!");
//		dot();
//		i = i.next();
//		final Identifier ix = i;
//		dhash1.getPersistExecutor().submit(new Runnable() {
//			public void run() {
//				dhash1.getStorage().put(new Entry(ix, "plupp", 0, 0, 0, 0, "null", null), null, new LockingStorage.Returner<Object>() {
//					public void call(Object b) {
//						q.add(new Object());
//					}
//				});
//			}
//		}).get();
//		if (getEmpty(dhash1, i) == null)
//			throw new RuntimeException("writing failed!");
//
//		dot();
//		q.take();
//		dhash1.setEnableGC(true);
//		dhash1.resetGC();
//		dhash1.gc();
//		while (dhash1.runningGC()) {
//			Thread.sleep(100);
//		}
//		dhash1.setEnableGC(false);
//		Object e = getEmpty(dhash1, i);
//		if (e != null)
//			throw new RuntimeException("gc didnt clean old null entries! has " + e);
//
//		dhash1.setEnableGC(true);
//		dhash1.setEnableClean(true);
//		dhash1.setEnableSynchronize(true);
//
//		System.out.println("done!");
//	}
//
//	public static void testReceiverCleaning() throws Exception {
//		System.out.print("Testing receiver cleaning");
//		final List<Object> l = new LinkedList<Object>();
//		Command p = new PingCommand(dhash1.getServerInfo());
//		dhash1.register(p, new Receiver<PingCommand>() {
//			public long getTimeout() {
//				return 1000 * 60 * 60;
//			}
//
//			public void receive(PingCommand command) {
//				l.add(new Object());
//			}
//		});
//		dot();
//		dhash1.cleanWaitingCommands();
//		dot();
//		dhash1.deliver(p);
//		if (l.size() != 1)
//			throw new RuntimeException("new commands cleaned!");
//		dot();
//		dhash1.register(new PingCommand(dhash1.getServerInfo()), new Receiver<PingCommand>() {
//			public long getTimeout() {
//				return 0L;
//			}
//
//			public void receive(PingCommand command) {
//				l.add(new Object());
//			}
//		});
//		dot();
//		dhash1.cleanWaitingCommands();
//		dot();
//		dhash1.deliver(p);
//		dot();
//		if (l.size() != 1)
//			throw new RuntimeException("old commands not cleaned!");
//		System.out.println("done!");
//	}
//
//	public static void testPrimitiveParameters(Dhasher d) throws Exception {
//		System.out.print("\tTesting primitive parameters");
//		dot();
//		Identifier i = Identifier.random();
//		dot();
//		d.put(i, new ArrayList()).get();
//		dot();
//		if (!d.exec(i, "size").get().equals(new Integer(0)))
//			throw new RuntimeException("Wrong exec response");
//		dot();
//		d.exec(i, "add", "gnu").get();
//		dot();
//		if (!d.exec(i, "size").get().equals(new Integer(1)))
//			throw new RuntimeException("wrong exec response!");
//		dot();
//		if (!d.exec(i, "get", new Integer(0)).get().equals("gnu"))
//			throw new RuntimeException("wrong exec response!");
//		dot();
//		if (!d.exec(i, "get", 0).get().equals("gnu"))
//			throw new RuntimeException("wrong exec response!");
//		System.out.println("done!");
//	}
//
//	public static void testTakeReplace(Dhasher d) throws Exception {
//		System.out.print("\tTesting take/replace");
//		dot();
//		if (d.take("kanas").get() != null)
//			throw new RuntimeException("take returns non null");
//		dot();
//		d.put("kanas", "knupp").get();
//		dot();
//		if (!d.take("kanas").get().equals("knupp"))
//			throw new RuntimeException("take returns wrong value!");
//		dot();
//		if (d.has("kanas").get().booleanValue())
//			throw new RuntimeException("take didnt remove the value!");
//		dot();
//		if (d.replace("kanas2", "knupp", "hepp").get().booleanValue())
//			throw new RuntimeException("replace works when it shouldnt!");
//		dot();
//		if (!d.replace("kanas2", null, "knupp").get().booleanValue())
//			throw new RuntimeException("replace didnt work when it should!");
//		dot();
//		if (d.replace("kanas2", "knapp", "hepp").get().booleanValue())
//			throw new RuntimeException("replace works when it shouldnt!");
//		dot();
//		if (d.replace("kanas2", null, "hepp").get().booleanValue())
//			throw new RuntimeException("replace works when it shouldnt!");
//		dot();
//		if (!d.replace("kanas2", "knupp", "hipp").get().booleanValue())
//			throw new RuntimeException("replace didnt work when it ought to!");
//		dot();
//		if (!d.get("kanas2").get().equals("hipp"))
//			throw new RuntimeException("replace didint replace the value! i wanted 'hipp' but got " + d.get("kanas2").get());
//
//		System.out.println("done!");
//	}
//
//	public static void testNextEntry(Dhasher d) throws Exception {
//		System.out.print("\tTesting nextEntry");
//		destroy();
//		dot();
//		HashMap<Identifier, String> tree = new HashMap<Identifier, String>();
//		for (int i = 0; i < 20; i++) {
//			d.put("key" + i, "value" + i).get();
//			tree.put(Identifier.generate("key" + i), "value" + i);
//			dot();
//		}
//		Delay<Map.Entry<Identifier, String>> nextEntryDelay = d.nextEntry(new Identifier(0).previous());
//		HashMap<Identifier, String> found = new HashMap<Identifier, String>();
//		for (int i = 0; i < 20; i++) {
//			if (!tree.containsKey(nextEntryDelay.get().getKey()))
//				throw new RuntimeException("bad entry returned, got " + nextEntryDelay.get());
//			String val = tree.remove(nextEntryDelay.get().getKey());
//			if (!val.equals(nextEntryDelay.get().getValue()))
//				throw new RuntimeException("bad content!");
//			if (found.containsKey(nextEntryDelay.get().getKey()))
//				throw new RuntimeException("already seen this content");
//			found.put(nextEntryDelay.get().getKey(), nextEntryDelay.get().getValue());
//			dot();
//			nextEntryDelay = d.nextEntry(nextEntryDelay.get().getKey());
//		}
//		nextEntryDelay = d.nextEntry(nextEntryDelay.get().getKey());
//		if (!found.containsKey(nextEntryDelay.get().getKey()))
//			throw new RuntimeException("not seen this key before");
//		if (!tree.isEmpty()) {
//			throw new RuntimeException("iteration missed some elements?");
//		}
//		dot();
//		System.out.println("done!");
//	}
//
//	public static String driver = "org.hsqldb.jdbcDriver";
//
//	public static String url = "jdbc:hsqldb:mem:test";
//
//	public static void setup(String[] arguments) throws Exception {
//		System.out.print("Starting new test nodes");
//		InetSocketAddress firstAddress = new InetSocketAddress("0.0.0.0", 20000);
//		dhash1 = ((DHash) new DHash().setServiceName("test").setLocal(firstAddress)).setDelayFactor(1).setInitialDelay(1)
//				.setStorage(new JDBCStorage(new JDBC(driver, url + "1")));
//		dhash1.start();
//		dot();
//		dhash2 = ((DHash) new DHash().setServiceName("test").setLocal(new InetSocketAddress("0.0.0.0", 20001)).setBootstrap(firstAddress))
//				.setDelayFactor(1).setInitialDelay(1).setStorage(new JDBCStorage(new JDBC(driver, url + "2")));
//		dhash2.start();
//		dot();
//		dhash3 = ((DHash) new DHash().setServiceName("test").setLocal(new InetSocketAddress("0.0.0.0", 20002)).setBootstrap(firstAddress))
//				.setDelayFactor(1).setInitialDelay(1).setStorage(new JDBCStorage(new JDBC(driver, url + "3")));
//		dhash3.start();
//		dot();
//		dhash4 = ((DHash) new DHash().setServiceName("test").setLocal(new InetSocketAddress("0.0.0.0", 20003)).setBootstrap(firstAddress))
//				.setDelayFactor(1).setInitialDelay(1).setStorage(new JDBCStorage(new JDBC(driver, url + "4")));
//		dhash4.start();
//		dot();
//		dhash5 = ((DHash) new DHash().setServiceName("test").setLocal(new InetSocketAddress("0.0.0.0", 20004)).setBootstrap(firstAddress))
//				.setDelayFactor(1).setInitialDelay(1).setStorage(new JDBCStorage(new JDBC(driver, url + "5")));
//		dhash5.start();
//		System.out.println("done!");
//
//		dhashes = new DHash[] { dhash1, dhash2, dhash3, dhash4, dhash5 };
//		Arrays.sort(dhashes, new Comparator<DHash>() {
//			public int compare(DHash a, DHash b) {
//				return a.getIdentifier().compareTo(b.getIdentifier());
//			}
//		});
//
//		testJoin();
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//
//		if (arguments.length == 1 && arguments[0].equals("pged")) {
//			while (true) {
//				testPutGetExecDel(new Dhasher(dhash1));
//			}
//		} else if (arguments.length == 1 && arguments[0].equals("locking")) {
//			while (true) {
//				testLocking(new Dhasher(dhashes[0]));
//			}
//		} else if (arguments.length == 1 && arguments[0].equals("replace")) {
//			while (true) {
//				testTakeReplace(new Dhasher(dhashes[0]));
//			}
//		} else {
//
//			testGC();
//
//			TestTransactions.test(arguments);
//
//			testCleanAndSynchronize();
//
//			Collection<Dhasher> dhashers = new ArrayList<Dhasher>();
//			dhashers.add(new RemoteDhasher(dhash2.getSocketServer().getServerSocket().socket().getLocalSocketAddress()));
//			dhashers.add(new Dhasher(dhash2));
//			for (Dhasher dhasher : dhashers) {
//
//				if (dhasher instanceof RemoteDhasher)
//					System.out.println("Testing with remote dhasher");
//				else
//					System.out.println("Testing with local dhasher");
//
//				testNextEntry(dhasher);
//
//				testPutGetExecDel(dhasher);
//
//				testGlobalCleaning(dhash2, dhasher);
//
//				testSynchronize(dhasher);
//
//				testRedundancy(dhasher);
//
//				testLocking(dhasher);
//
//				testPrimitiveParameters(dhasher);
//
//				testTakeReplace(dhasher);
//
//				testEnvoys(dhasher);
//
//				testTimeouts(dhasher);
//
//				testModifyingExec(dhash2, dhasher);
//
//				testRemove(dhasher);
//
//				testChainedExecs(dhasher);
//
//			}
//
//			TestDTree.test(arguments);
//
//			TestDSet.test(arguments);
//
//			TestDList.test(arguments);
//
//			TestDCollections.test(arguments);
//
//			TestDTreap.test(arguments);
//
//			TestDHashMap.test(arguments);
//
//			TestDMaps.test(arguments);
//
//			testReceiverCleaning();
//
//		}
//
//		teardown();
//	}
//
//	public static void teardown() {
//		dhash1.stop();
//		dhash2.stop();
//		dhash3.stop();
//		dhash4.stop();
//		dhash5.stop();
//
//		exit();
//	}
//
//}
