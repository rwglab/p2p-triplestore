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
//import cx.ath.troja.chordless.dhash.Entry;
//import cx.ath.troja.chordless.dhash.storage.LockingStorage;
//import cx.ath.troja.nja.Identifier;
//
//public class TestStorage extends TestAll {
//
//	public static void testPutDel() throws Exception {
//		System.out.print("Testing put/del");
//		LockingStorage s = LockingStorage.getInstance("cx.ath.troja.chordless.dhash.storage.JDBCStorage", "org.sqlite.JDBC",
//				"jdbc:sqlite:testStorage.db");
//		long t = System.currentTimeMillis();
//		for (int i = 0; i < 1000; i++) {
//			Entry e = new Entry(Identifier.generate("hej" + i), "hoho");
//			put(s, e);
//			System.out.print(".");
//		}
//		System.out.print("(avg put " + ((System.currentTimeMillis() - t) / 1000.0) + ")");
//		t = System.currentTimeMillis();
//		for (int i = 0; i < 1000; i++) {
//			del(s, Identifier.generate("hej" + i));
//			System.out.print(".");
//		}
//		System.out.print("(avg del " + ((System.currentTimeMillis() - t) / 1000.0) + ")");
//		System.out.println("done!");
//	}
//
//	public static void main(String[] arguments) throws Exception {
//		testPutDel();
//	}
//}