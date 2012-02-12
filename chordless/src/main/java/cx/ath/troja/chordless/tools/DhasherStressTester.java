/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.tools;

import java.net.InetSocketAddress;

import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Dhasher;
import cx.ath.troja.chordless.dhash.RemoteDhasher;
import cx.ath.troja.chordless.dhash.storage.JDBCStorage;
import cx.ath.troja.chordless.dhash.structures.DHashMap;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.JDBC;

public class DhasherStressTester {

	public static void main(String[] arguments) throws Exception {
		if (arguments.length < 2) {
			System.err.println("Usage: DatabaseStressTester [HOST PORT] N TYPE(put|dmap|exec|trans|transexec|innertrans)");
			System.exit(1);
		} else {
			if (arguments.length == 4) {
				DhasherStressTester t = new DhasherStressTester(arguments[0], Integer.parseInt(arguments[1]));
				t.stress(Long.parseLong(arguments[2]), arguments[3]);
				t.stop();
			} else if (arguments.length == 2) {
				DhasherStressTester t = new DhasherStressTester();
				t.stress(Long.parseLong(arguments[0]), arguments[1]);
			}
		}
	}

	private Dhasher dhasher;

	private DHash dhash;

	public DhasherStressTester(String host, int port) {
		dhasher = new RemoteDhasher(new InetSocketAddress(host, port));
	}

	public DhasherStressTester() {
		dhash = new DHash().setStorage(new JDBCStorage(new JDBC("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:dhash.db;shutdown=true")));
		dhash.start();
		dhasher = new Dhasher(dhash);
	}

	public void stop() {
		if (dhasher instanceof RemoteDhasher) {
			((RemoteDhasher) dhasher).stop();
		} else {
			dhash.stop();
		}
	}

	public void stress(long n, String type) {
		long t = System.currentTimeMillis();
		int middle = 0;
		long mtime = System.currentTimeMillis();
		long t1 = 0;
		long t2 = 0;
		long t3 = 0;
		long t4 = 0;
		long t5 = 0;
		int middlesize = 200;
		DHashMap dm = new DHashMap();
		if (type.equals("dmap")) {
			dhasher.put(dm).get();
		} else if (type.equals("exec") || type.equals("transexec") || type.equals("innertrans")) {
			dhasher.put(Identifier.getMAX_IDENTIFIER(), "blapp").get();
		}
		for (long i = 0; i < n; i++) {
			if (type.equals("put")) {
				dhasher.put("blaj" + i, "japp" + i).get();
			} else if (type.equals("dmap")) {
				dhasher.exec(dm.getIdentifier(), "put", "key" + i, "value" + i).get();
			} else if (type.equals("exec")) {
				dhasher.exec(Identifier.getMAX_IDENTIFIER(), "toString").get();
			} else if (type.equals("trans")) {
				Transaction trans = dhasher.transaction();
				trans.put(Identifier.getMAX_IDENTIFIER(), "blaj" + i).get();
				trans.commit();
			} else if (type.equals("transexec")) {
				Transaction trans = dhasher.transaction();
				trans.exec(Identifier.getMAX_IDENTIFIER(), "toString").get();
				trans.abort();
			} else if (type.equals("innertrans")) {
				t1 -= System.currentTimeMillis();
				Transaction trans = dhasher.transaction();
				t1 += System.currentTimeMillis();
				t2 -= System.currentTimeMillis();
				Transaction inner = trans.transaction();
				t2 += System.currentTimeMillis();
				t3 -= System.currentTimeMillis();
				inner.put(Identifier.getMAX_IDENTIFIER(), "blaj" + i).get();
				t3 += System.currentTimeMillis();
				t4 -= System.currentTimeMillis();
				inner.commit();
				t4 += System.currentTimeMillis();
				t5 -= System.currentTimeMillis();
				trans.commit();
				t5 += System.currentTimeMillis();
			} else {
				throw new RuntimeException("unknown type " + type);
			}
			middle++;
			if (middle > middlesize) {
				long diff = System.currentTimeMillis() - mtime;
				System.out.println("\n@" + i + ", last " + middlesize + ": " + diff + "ms (" + (diff / middlesize) + "ms/op)");
				middle = 1;
				mtime = System.currentTimeMillis();
			}
			System.out.print(".");
		}
		long tend = System.currentTimeMillis();
		System.out.println("OK (" + (tend - t) + "ms, " + ((tend - t) / n) + "ms/op)");
		System.out.println("t1: " + (t1 / n) + ", t2: " + (t2 / n) + ", t3: " + (t3 / n) + ", t4: " + (t4 / n) + ", t5: " + (t5 / n));
	}

}
