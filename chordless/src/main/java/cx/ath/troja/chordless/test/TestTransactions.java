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
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.Callable;
//import java.util.logging.Level;
//
//import cx.ath.troja.chordless.dhash.DHash;
//import cx.ath.troja.chordless.dhash.Delay;
//import cx.ath.troja.chordless.dhash.Dhasher;
//import cx.ath.troja.chordless.dhash.Entry;
//import cx.ath.troja.chordless.dhash.Persistent;
//import cx.ath.troja.chordless.dhash.Persister;
//import cx.ath.troja.chordless.dhash.storage.ExecStorage;
//import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
//import cx.ath.troja.chordless.dhash.transactions.CompromisedTransactionException;
//import cx.ath.troja.chordless.dhash.transactions.Transaction;
//import cx.ath.troja.chordless.dhash.transactions.TransactionBackend;
//import cx.ath.troja.nja.FutureValue;
//import cx.ath.troja.nja.Identifier;
//import cx.ath.troja.nja.Log;
//
//public class TestTransactions extends TestDHash {
//
//	public static void testAbortedTransactions(Persister d, Transaction t) throws Exception {
//		System.out.print("\tTesting aborted transactions");
//
//		t.put("plupp", new HashMap()).get();
//		dot();
//		t.put("plapp", new HashMap()).get();
//		dot();
//		t.exec("plupp", "put", "key1", "value1").get();
//		dot();
//		t.commute("plapp", "put", "key2", "value2").get();
//		dot();
//
//		Delay<Integer> delay = t.exec("plupp", "size");
//		if (delay.get().intValue() != 1)
//			throw new RuntimeException("wrong size in transaction");
//		dot();
//		delay = t.exec("plapp", "size");
//		if (delay.get().intValue() != 1)
//			throw new RuntimeException("wrong size in transaction");
//		dot();
//
//		if (d.get("plupp").get() != null)
//			throw new RuntimeException("object exists outside transaction!");
//		if (d.get("plapp").get() != null)
//			throw new RuntimeException("object exists outside transaction!");
//
//		t.abort();
//
//		if (d.get("plupp").get() != null)
//			throw new RuntimeException("object exists after aborted transaction!");
//		if (d.get("plapp").get() != null)
//			throw new RuntimeException("object exists after aborted transaction!");
//
//		System.out.println("done!");
//	}
//
//	public static Map<String, String> testCommitedTransaction(Persister d, Transaction t) throws Exception {
//		System.out.print("\tTesting commited transaction");
//
//		t.put("plupp", new HashMap()).get();
//		dot();
//		t.put("plapp", new HashMap()).get();
//		dot();
//		t.exec("plupp", "put", "key1", "value1").get();
//		dot();
//		t.commute("plapp", "put", "key1", "value1").get();
//		dot();
//
//		Delay<Integer> delay = t.exec("plupp", "size");
//		if (delay.get().intValue() != 1)
//			throw new RuntimeException("wrong size in transaction");
//		dot();
//		delay = t.exec("plapp", "size");
//		if (delay.get().intValue() != 1)
//			throw new RuntimeException("wrong size in transaction");
//		dot();
//
//		if (d.get("plupp").get() != null)
//			throw new RuntimeException("object exists outside transaction!");
//		if (d.get("plapp").get() != null)
//			throw new RuntimeException("object exists outside transaction!");
//		dot();
//
//		t.commit();
//		dot();
//
//		Map<String, String> map1 = new HashMap<String, String>();
//		map1.put("key1", "value1");
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist after commited transaction!");
//		if (!d.get("plapp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist after commited transaction!");
//
//		System.out.println("done!");
//
//		return map1;
//	}
//
//	public static void testAbortedTransactionWithData(Persister d, Transaction t, Map<String, String> map1) throws Exception {
//		System.out.print("\tTesting aborted transaction with preset data");
//
//		if (!t.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist in fresh transaction!");
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist outside fresh transaction!");
//		dot();
//		if (!t.get("plapp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist in fresh transaction!");
//		if (!d.get("plapp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist outside fresh transaction!");
//		dot();
//
//		t.exec("plupp", "put", "key3", "value3").get();
//		dot();
//		t.commute("plupp", "put", "key4", "value4").get();
//		dot();
//		t.commute("plapp", "put", "key3", "value3").get();
//		dot();
//		t.commute("plapp", "put", "key4", "value4").get();
//		dot();
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt remain unchanged in main db when changed in transaction! expected " + map1 + " got "
//					+ d.get("plupp").get());
//		dot();
//		if (!d.get("plapp").get().equals(map1))
//			throw new RuntimeException("object doesnt remain unchanged in main db when changed in transaction! expected " + map1 + " got "
//					+ d.get("plupp").get());
//		dot();
//
//		map1.put("key3", "value3");
//		dot();
//		map1.put("key4", "value4");
//		dot();
//
//		if (!t.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt change in transaction!");
//		dot();
//		if (!t.get("plapp").get().equals(map1))
//			throw new RuntimeException("object doesnt change in transaction!");
//		dot();
//
//		t.abort();
//		dot();
//
//		map1.remove("key3");
//		dot();
//		map1.remove("key4");
//		dot();
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt remain unchanged in main db after aborted transaction");
//		dot();
//		if (!d.get("plapp").get().equals(map1))
//			throw new RuntimeException("object doesnt remain unchanged in main db after aborted transaction");
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static void testConflictedTransaction(Persister d, Transaction t, Map<String, String> map1) throws Exception {
//		System.out.print("\tTesting conflicted transaction");
//
//		if (!t.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt exist in fresh transaction");
//		dot();
//
//		t.exec("plupp", "put", "key4", "value4").get();
//		dot();
//
//		d.commute("plupp", "put", "key5", "value5").get();
//		dot();
//
//		map1.put("key4", "value4");
//
//		if (!t.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt change in transaction!");
//		dot();
//
//		map1.remove("key4");
//		map1.put("key5", "value5");
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt change in db!");
//		dot();
//
//		try {
//			t.commit();
//			throw new RuntimeException("commiting compromised transaction doesnt raise exception!");
//		} catch (CompromisedTransactionException e) {
//			dot();
//		}
//
//		System.out.println("done!");
//	}
//
//	public static void testConflictingTransactions(Persister d, Transaction t, Transaction t2, Map<String, String> map1) throws Exception {
//		System.out.print("\tTesting conflicting transactions");
//
//		t.exec("plupp", "put", "key6", "value6").get();
//		t2.exec("plupp", "put", "key7", "value7").get();
//		t.exec("plupp", "put", "key8", "value8").get();
//		t2.exec("plupp", "put", "key9", "value9").get();
//		dot();
//
//		map1.put("key6", "value6");
//		map1.put("key8", "value8");
//		if (!t.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt change in transaction!");
//		dot();
//		map1.remove("key6");
//		map1.remove("key8");
//
//		map1.put("key7", "value7");
//		map1.put("key9", "value9");
//		if (!t2.get("plupp").get().equals(map1))
//			throw new RuntimeException("object doesnt change in transaction!");
//		dot();
//		map1.remove("key7");
//		map1.remove("key9");
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("objected changed in main db!");
//
//		t.commit();
//		dot();
//
//		map1.put("key6", "value6");
//		map1.put("key8", "value8");
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("objected not changed after commited transaction!");
//
//		map1.remove("key6");
//		map1.remove("key8");
//		map1.put("key7", "value7");
//		map1.put("key9", "value9");
//
//		if (!t2.get("plupp").get().equals(map1))
//			throw new RuntimeException("object changed in other transaction!");
//		dot();
//
//		try {
//			t2.commit();
//			throw new RuntimeException("commiting compromised transaction doesnt raise exception!");
//		} catch (CompromisedTransactionException e) {
//			dot();
//		}
//
//		map1.remove("key7");
//		map1.remove("key9");
//		map1.put("key6", "value6");
//		map1.put("key8", "value8");
//
//		if (!d.get("plupp").get().equals(map1))
//			throw new RuntimeException("objected changed after commiting failed transaction!");
//
//		System.out.println("done!");
//	}
//
//	public static void testConflictFree(Persister d, Transaction t, Transaction t2) throws Exception {
//		System.out.print("\tTesting conflict free reads");
//		t.get("apa").get();
//		dot();
//		t2.get("apa").get();
//		dot();
//		t2.put("apa", "gnu").get();
//		dot();
//		if (!t2.get("apa").get().equals("gnu"))
//			throw new RuntimeException("new value not present in transaction");
//		dot();
//		t.commit();
//		dot();
//		t2.commit();
//		dot();
//		if (!d.get("apa").get().equals("gnu"))
//			throw new RuntimeException("new value not present after commit");
//		dot();
//
//		t = d.transaction();
//		t2 = d.transaction();
//		dot();
//		t.put("apa", "gnu").get();
//		dot();
//		t2.get("apa").get();
//		dot();
//		t2.commit();
//		dot();
//		t.commit();
//		dot();
//
//		t = d.transaction();
//		t2 = d.transaction();
//		dot();
//		t.put("apa", "gnu").get();
//		dot();
//		t2.get("apa").get();
//		dot();
//		t.commit();
//		dot();
//		try {
//			t2.commit();
//			throw new RuntimeException("should not commit");
//		} catch (CompromisedTransactionException e) {
//			dot();
//		}
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static void cleanTransactions() throws Exception {
//		List<FutureValue<Object>> waits = new ArrayList<FutureValue<Object>>();
//		for (int i = 0; i < dhashes.length; i++) {
//			while (dhashes[i].runningTransactionCleanup())
//				Thread.sleep(20);
//			final FutureValue<Object> wait = new FutureValue<Object>();
//			final DHash d = dhashes[i];
//			waits.add(wait);
//			d.getPersistExecutor().execute(new Runnable() {
//				public void run() {
//					d.cleanTransactions();
//					wait.set("done");
//				}
//			});
//		}
//		for (FutureValue<Object> wait : waits) {
//			wait.get();
//		}
//		for (int i = 0; i < dhashes.length; i++) {
//			while (dhashes[i].runningTransactionCleanup())
//				Thread.sleep(20);
//		}
//		Thread.sleep(200);
//	}
//
//	public static boolean locked(Object key) throws Exception {
//		final Identifier id = Identifier.generate(key);
//		for (int i = 0; i < dhashes.length; i++) {
//			if (id.betweenGT_LTE(dhashes[i].getPredecessor().getIdentifier(), dhashes[i].getIdentifier())) {
//				final int x = i;
//				Entry entry = dhashes[i].getPersistExecutor().submit(new Callable<Entry>() {
//					public Entry call() {
//						return dhashes[x].getStorage().getEmpty(id);
//					}
//				}).get();
//				if (entry == null)
//					throw new NoSuchEntryException(id, true);
//				return entry.getLocker() != null;
//			}
//		}
//		throw new NoSuchEntryException(id, true);
//	}
//
//	public static void testRecoveryAborted() throws Exception {
//		System.out.print("\tTesting recovery of ABORTED transactions");
//		Dhasher d = new Dhasher(dhash1);
//		Transaction t = d.transaction();
//		dot();
//		dot();
//		d.exec(t.transactionBackend(), "_breakAbort", new Long(10000));
//		dot();
//		Thread.sleep(100);
//		dot();
//		TransactionBackend b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b.getState() != TransactionBackend.ABORTED)
//			throw new RuntimeException("wrong state or deleted transaction!");
//		dot();
//		cleanTransactions();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b == null)
//			throw new RuntimeException("active aborted transaction cleaned!");
//
//		t = d.transaction();
//
//		dot();
//		d.exec(t.transactionBackend(), "_breakAbort", new Long(0)).get();
//		dot();
//		cleanTransactions();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		dot();
//		if (b != null)
//			throw new RuntimeException("dead aborted transaction not cleaned!");
//
//		System.out.println("done!");
//	}
//
//	public static void testRecoveryCommited() throws Exception {
//		System.out.print("\tTesting recovery of COMMITED transactions");
//		destroy();
//		Dhasher d = new Dhasher(dhash1);
//		HashMap<String, String> m = new HashMap<String, String>();
//		m.put("knas", "knes");
//		d.put("blapp", "1").get();
//		d.put("hepp", "2").get();
//		d.put("mappen", m).get();
//		dot();
//		Transaction t = d.transaction();
//
//		dot();
//		t.put("blapp", "blepp").get();
//		dot();
//		t.put("hepp", "happ").get();
//		dot();
//		t.commute("mappen", "put", "knas", "knes").get();
//		dot();
//		Map<Identifier, Object> m2 = new HashMap<Identifier, Object>();
//		Identifier i = Identifier.generate("blapp");
//		m2.put(i, new Entry(i, "blepp"));
//		dot();
//		d.exec(t.transactionBackend(), "_breakCommit", m2, new Long(10000));
//		dot();
//		Thread.sleep(100);
//		dot();
//		TransactionBackend b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b.getState() != TransactionBackend.COMMITED)
//			throw new RuntimeException("wrong state or deleted transaction!");
//		dot();
//		cleanTransactions();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b == null)
//			throw new RuntimeException("active commited transaction cleaned!");
//
//		destroy();
//
//		d.put("blapp", "1").get();
//		d.put("hepp", "2").get();
//		d.put("mappen", m).get();
//
//		t = d.transaction();
//
//		Log.getLogger("cx.ath.troja.chordless.dhash.storage.JDBCStorage").setLevel(Level.SEVERE);
//
//		dot();
//		t.put("blapp", "blepp").get();
//		dot();
//		t.put("hepp", "happ").get();
//		dot();
//		t.commute("mappen", "put", "knas", "knes").get();
//		dot();
//		d.exec(t.transactionBackend(), "_breakCommit", m2, new Long(0)).get();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b.getState() != TransactionBackend.COMMITED)
//			throw new RuntimeException("wrong state or deleted transaction!");
//		dot();
//		if (locked("blapp"))
//			throw new RuntimeException("entry locked!");
//		dot();
//		if (!locked("hepp"))
//			throw new RuntimeException("entry not locked!");
//		dot();
//		if (!locked("mappen"))
//			throw new RuntimeException("entry not locked!");
//		dot();
//		cleanTransactions();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b != null)
//			throw new RuntimeException("inactive commited transaction not cleaned!");
//		dot();
//		if (locked("blapp"))
//			throw new RuntimeException("entry locked!");
//		dot();
//		if (locked("hepp"))
//			throw new RuntimeException("entry locked!");
//		dot();
//		if (locked("mappen"))
//			throw new RuntimeException("entry locked!");
//		dot();
//		if (!d.get("blapp").get().equals("blepp"))
//			throw new RuntimeException("entry didnt change");
//		dot();
//		if (!d.get("hepp").get().equals("happ"))
//			throw new RuntimeException("entry didnt change");
//		m.put("knas", "knes");
//		if (!d.get("mappen").get().equals(m))
//			throw new RuntimeException("entry didnt change");
//
//		System.out.println("done!");
//	}
//
//	public static void testRecoveryPrepared() throws Exception {
//		System.out.print("\tTesting recovery of PREPARED transactions");
//		destroy();
//		Dhasher d = new Dhasher(dhash1);
//		HashMap<String, String> m = new HashMap<String, String>();
//		m.put("knas", "knes");
//		d.put("blapp", "1").get();
//		d.put("hepp", "2").get();
//		d.put("mappen", m).get();
//		dot();
//		Transaction t = d.transaction();
//
//		dot();
//		t.put("blapp", "blepp").get();
//		dot();
//		t.put("hepp", "happ").get();
//		dot();
//		t.commute("mappen", "put", "knas", "knes").get();
//		dot();
//		d.exec(t.transactionBackend(), "_breakPrepare", Arrays.asList(new Object[] { Identifier.generate("blapp") }), new Long(10000));
//		dot();
//		Thread.sleep(100);
//		dot();
//		TransactionBackend b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b.getState() != TransactionBackend.PREPARED)
//			throw new RuntimeException("wrong state or deleted transaction!");
//		dot();
//		cleanTransactions();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b == null)
//			throw new RuntimeException("active prepared transaction cleaned!");
//
//		destroy();
//
//		d.put("blapp", "1").get();
//		d.put("hepp", "2").get();
//		d.put("mappen", m).get();
//
//		t = d.transaction();
//
//		dot();
//		t.put("blapp", "blepp").get();
//		dot();
//		t.put("hepp", "happ").get();
//		dot();
//		t.commute("mappen", "put", "knas", "knes").get();
//		dot();
//		d.exec(t.transactionBackend(), "_breakPrepare", Arrays.asList(new Object[] { Identifier.generate("blapp") }), new Long(0)).get();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b.getState() != TransactionBackend.PREPARED)
//			throw new RuntimeException("wrong state or deleted transaction!");
//		dot();
//		if (!locked("blapp"))
//			throw new RuntimeException("entry not locked!");
//		dot();
//		cleanTransactions();
//		dot();
//		b = (TransactionBackend) d.get(t.transactionBackend()).get();
//		if (b != null)
//			throw new RuntimeException("inactive prepared transaction not cleaned!");
//		dot();
//		if (locked("blapp"))
//			throw new RuntimeException("entry locked!");
//		dot();
//		if (!d.get("blapp").get().equals("1"))
//			throw new RuntimeException("entry not changed");
//		dot();
//		if (!d.get("hepp").get().equals("2"))
//			throw new RuntimeException("entry not changed");
//		if (!d.get("mappen").get().equals(m))
//			throw new RuntimeException("entry not changed");
//		dot();
//
//		System.out.println("done!");
//	}
//
//	public static void testRecoveryStarted() throws Exception {
//		System.out.print("\tTesting recovery of STARTED transactions");
//		Dhasher d = new Dhasher(dhash1);
//		Transaction t = d.transaction();
//		dot();
//		if (!t.active())
//			throw new RuntimeException("transaction broken after creation");
//		dot();
//		cleanTransactions();
//		dot();
//		if (!t.active())
//			throw new RuntimeException("new transaction cleaned");
//		dot();
//		d.exec(t.transactionBackend(), "_setCreatedAt", new Long(0)).get();
//		dot();
//		cleanTransactions();
//		dot();
//		if (t.active())
//			throw new RuntimeException("old transaction not cleaned");
//		System.out.println("done!");
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		setup(arguments);
//		test(arguments);
//		teardown();
//	}
//
//	public static void testInvalidityOfRecoveredTransactions() throws Exception {
//		System.out.print("Testing that recovered transactions are invalid unless PREPARED or COMMITED");
//		Log.getLogger("cx.ath.troja.chordless.dhash.commands.ExecCommand").setLevel(Level.SEVERE);
//		Dhasher d = new Dhasher(dhash2);
//		dot();
//		Transaction t = d.transaction();
//		dot();
//		t.put("hej", "haj").get();
//		dot();
//		for (int i = 0; i < dhashes.length; i++) {
//			((ExecStorage) dhashes[i].getStorage())._break_by_forgetting(t.transactionBackend());
//			TransactionBackend._removeBackend(t.transactionBackend());
//		}
//		dot();
//		try {
//			t.put("bapp", "blur").get();
//			throw new RuntimeException("should raise DeadTransactionException!");
//		} catch (Transaction.DeadTransactionException e) {
//			dot();
//		}
//		Log.getLogger("cx.ath.troja.chordless.dhash.commands.ExecCommand").setLevel(Level.INFO);
//		System.out.println("done!");
//	}
//
//	public static class Counter extends Persistent {
//		private long count = 0;
//
//		private Identifier id = Identifier.random();
//
//		public Object getId() {
//			return id;
//		}
//
//		public String toString() {
//			return "Counter id=" + id + " count=" + count;
//		}
//
//		public void inc() {
//			count++;
//		}
//
//		public long get() {
//			setTaint(false);
//			return count;
//		}
//	}
//
//	public static void ensureCompromised(Transaction t) throws Exception {
//		try {
//			t.commit();
//			throw new RuntimeException("transaction should fail!");
//		} catch (CompromisedTransactionException e) {
//			dot();
//		}
//	}
//
//	public static void assureCount(Persister p, Identifier i, long l) {
//		if (!p.exec(i, "get").get().equals(new Long(l)))
//			throw new RuntimeException("wrong count, wanted " + l + " but got " + p.exec(i, "get").get());
//		dot();
//	}
//
//	public static void testCommute(Persister d) throws Exception {
//		System.out.print("\tTesting commutativity");
//		Counter c = new Counter();
//		Identifier i = c.getIdentifier();
//		d.put(i, c).get();
//		dot();
//		d.exec(i, "inc").get();
//		dot();
//		assureCount(d, i, 1);
//		d.commute(i, "inc").get();
//		assureCount(d, i, 2);
//
//		Transaction t1 = d.transaction();
//		t1.exec(i, "inc").get();
//		dot();
//
//		assureCount(t1, i, 3);
//		d.exec(i, "inc").get();
//		dot();
//		ensureCompromised(t1);
//		assureCount(d, i, 3);
//
//		t1 = d.transaction();
//		t1.exec(i, "inc").get();
//		dot();
//		assureCount(t1, i, 4);
//		d.commute(i, "inc").get();
//		dot();
//		ensureCompromised(t1);
//		assureCount(d, i, 4);
//
//		t1 = d.transaction();
//		t1.commute(i, "inc").get();
//		dot();
//		assureCount(t1, i, 5);
//		d.exec(i, "inc").get();
//		dot();
//		ensureCompromised(t1);
//		assureCount(d, i, 5);
//
//		t1 = d.transaction();
//		t1.commute(i, "inc").get();
//		dot();
//		assureCount(t1, i, 6);
//		d.commute(i, "inc").get();
//		dot();
//		assureCount(d, i, 6);
//		t1.commit();
//		dot();
//		assureCount(d, i, 7);
//
//		t1 = d.transaction();
//		Transaction t2 = d.transaction();
//		dot();
//		t1.exec(i, "inc").get();
//		dot();
//		assureCount(t1, i, 8);
//		t2.exec(i, "inc").get();
//		dot();
//		assureCount(t2, i, 8);
//		t1.commit();
//		dot();
//		ensureCompromised(t2);
//		assureCount(d, i, 8);
//
//		t1 = d.transaction();
//		t2 = d.transaction();
//		dot();
//		t1.exec(i, "inc").get();
//		dot();
//		assureCount(t1, i, 9);
//		t2.commute(i, "inc").get();
//		dot();
//		assureCount(t2, i, 9);
//		t1.commit();
//		dot();
//		ensureCompromised(t2);
//		assureCount(d, i, 9);
//
//		t1 = d.transaction();
//		t2 = d.transaction();
//		dot();
//		t1.commute(i, "inc").get();
//		dot();
//		assureCount(t1, i, 10);
//		t2.exec(i, "inc").get();
//		dot();
//		assureCount(t2, i, 10);
//		t1.commit();
//		dot();
//		ensureCompromised(t2);
//		assureCount(d, i, 10);
//
//		t1 = d.transaction();
//		t2 = d.transaction();
//		dot();
//		t1.commute(i, "inc").get();
//		dot();
//		assureCount(t1, i, 11);
//		t2.commute(i, "inc").get();
//		dot();
//		assureCount(t2, i, 11);
//		t1.commit();
//		dot();
//		t2.commit();
//		dot();
//		assureCount(d, i, 12);
//
//		t1 = d.transaction();
//		t2 = d.transaction();
//		dot();
//		t1.commute(i, "inc").get();
//		dot();
//		t1.commute(i, "inc").get();
//		dot();
//		assureCount(t1, i, 14);
//		t2.commute(i, "inc").get();
//		dot();
//		t2.commute(i, "inc").get();
//		dot();
//		assureCount(t2, i, 14);
//		t1.commit();
//		dot();
//		assureCount(d, i, 14);
//		t2.commit();
//		dot();
//		assureCount(d, i, 16);
//
//		System.out.println("done!");
//	}
//
//	public static void test(String[] arguments) throws Exception {
//		System.out.println("Testing transaction recovery");
//
//		testRecoveryCommited();
//		testRecoveryPrepared();
//		testRecoveryStarted();
//		testRecoveryAborted();
//
//		System.out.println("Testing transactions");
//
//		Dhasher d = new Dhasher(dhashes[0]);
//
//		testAbortedTransactions(d, d.transaction());
//		Map<String, String> comparison = testCommitedTransaction(d, d.transaction());
//		testAbortedTransactionWithData(d, d.transaction(), comparison);
//		testConflictedTransaction(d, d.transaction(), comparison);
//		testConflictingTransactions(d, d.transaction(), d.transaction(), comparison);
//		testConflictFree(d, d.transaction(), d.transaction());
//		testCommute(d);
//
//		System.out.println("Testing nested transactions");
//
//		destroy(false);
//
//		Transaction t = d.transaction();
//		testAbortedTransactions(t, t.transaction());
//		comparison = testCommitedTransaction(t, t.transaction());
//		testAbortedTransactionWithData(t, t.transaction(), comparison);
//		testConflictedTransaction(t, t.transaction(), comparison);
//		testConflictingTransactions(t, t.transaction(), t.transaction(), comparison);
//		testConflictFree(t, t.transaction(), t.transaction());
//		testCommute(t);
//
//		testInvalidityOfRecoveredTransactions();
//
//	}
//
//}