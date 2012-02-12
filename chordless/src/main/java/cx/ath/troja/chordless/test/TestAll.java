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
//import java.util.Map;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import cx.ath.troja.chordless.dhash.DHash;
//import cx.ath.troja.chordless.dhash.Entry;
//import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
//import cx.ath.troja.chordless.dhash.storage.LockingStorage;
//import cx.ath.troja.nja.Identifier;
//import cx.ath.troja.nja.ThreadState;
//
//public class TestAll {
//
//	public static boolean doExit = true;
//
//	public static void exit() {
//		if (doExit)
//			System.exit(0);
//	}
//
//	public static void dot() {
//		System.out.print(".");
//	}
//
//	public static Runnable putRunnable(final LockingStorage storage, final Entry entry, final LinkedBlockingQueue<Object> queue) throws Exception {
//		return new Runnable() {
//			public void run() {
//				ThreadState.put(ExecutorService.class, DHash.PERSIST_EXECUTOR);
//				storage.put(entry, null, new LockingStorage.Returner<Object>() {
//					public void call(Object b) {
//						queue.add(new Object());
//					}
//				});
//			}
//		};
//	}
//
//	public static Map<Identifier, Entry> getEmpty(final DHash d) throws Exception {
//		return d.getPersistExecutor().submit(new Callable<Map<Identifier, Entry>>() {
//			public Map<Identifier, Entry> call() {
//				return ((JDBCStorage) d.getStorage()).getEmpty();
//			}
//		}).get();
//	}
//
//	public static void put(LockingStorage storage, Entry entry) throws Exception {
//		LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
//		new Thread(putRunnable(storage, entry, queue)).start();
//		queue.take();
//	}
//
//	public static void put(DHash d, Entry entry) throws Exception {
//		LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
//		d.getPersistExecutor().submit(putRunnable(d.getStorage(), entry, queue)).get();
//		queue.take();
//	}
//
//	public static void put(final DHash d, final Collection<Entry> c) throws Exception {
//		final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
//		d.getPersistExecutor().submit(new Runnable() {
//			public void run() {
//				ThreadState.put(ExecutorService.class, DHash.PERSIST_EXECUTOR);
//				d.getStorage().put(c, null, new Runnable() {
//					public void run() {
//						queue.add(new Object());
//					}
//				});
//			}
//		}).get();
//		queue.take();
//	}
//
//	public static Runnable delRunnable(final LockingStorage storage, final Identifier identifier, final LinkedBlockingQueue<Object> queue)
//			throws Exception {
//		return new Runnable() {
//			public void run() {
//				ThreadState.put(ExecutorService.class, DHash.PERSIST_EXECUTOR);
//				storage.del(identifier, null, new LockingStorage.Returner<Boolean>() {
//					public void call(Boolean b) {
//						queue.add(new Object());
//					}
//				});
//			}
//		};
//	}
//
//	public static void del(LockingStorage storage, Identifier identifier) throws Exception {
//		LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
//		new Thread(delRunnable(storage, identifier, queue)).start();
//		queue.take();
//	}
//
//	public static void del(DHash d, Identifier identifier) throws Exception {
//		LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
//		d.getPersistExecutor().submit(delRunnable(d.getStorage(), identifier, queue)).get();
//		queue.take();
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		doExit = false;
//		TestDHash.main(arguments);
//		TestMerkleNode.main(arguments);
//		doExit = true;
//		exit();
//	}
//
//}