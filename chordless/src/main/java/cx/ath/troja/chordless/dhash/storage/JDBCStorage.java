/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */
package cx.ath.troja.chordless.dhash.storage;

import cx.ath.troja.chordless.ChordSet;
import cx.ath.troja.chordless.SingletonEventBus;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.MerkleNode;
import cx.ath.troja.chordless.event.DeleteEvent;
import cx.ath.troja.chordless.event.PutEvent;
import cx.ath.troja.chordless.event.ReadEvent;
import cx.ath.troja.chordless.event.UpdateEvent;
import cx.ath.troja.nja.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import static com.google.common.base.Throwables.propagate;
import static cx.ath.troja.nja.Log.error;
import static cx.ath.troja.nja.Log.warn;

public class JDBCStorage extends LockingStorage {

	private JDBC jdbc;

	public JDBCStorage(JDBC j) {
		jdbc = j;
		ensureTables();
	}

	public JDBCStorage(String klass, String url) {
		this(new JDBC(klass, url));
	}

	private void ensureTables() {
		/**
		 * Unique id of the entry.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "ID", String.class).primaryKey().ensure();
		/**
		 * The actual content of the entry.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "CONTENT", byte[].class).ensure();
		/**
		 * When the entry was created.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "TIMESTAMP", Long.class).ensure();
		/**
		 * What non-commutative version of the CONTENT this is.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "VERSION", Long.class).ensure();
		/**
		 * What commutative version of the CONTENT this is.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "COMMUTATION", Long.class).ensure();
		/**
		 * What version of the entire row this is.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "ITERATION", Long.class).ensure();
		/**
		 * What java klass is serialized in this entry.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "KLASS", String.class).indexed().ensure();
		/**
		 * Who has a lock on this entry.
		 */
		jdbc.col("CHORDLESS_ENTRIES", "LOCKEDBY", String.class).ensure();

		jdbc.col("CHORDLESS_MERKLE_NODES", "ID", String.class).primaryKey().ensure();
		jdbc.col("CHORDLESS_MERKLE_NODES", "PARENTID", String.class).indexed().ensure();
		jdbc.col("CHORDLESS_MERKLE_NODES", "HASH", String.class).ensure();
		jdbc.col("CHORDLESS_MERKLE_NODES", "ISLEAF", Integer.class).ensure();
	}

	private void sqlPut(Entry e) {
		ensurePersistExecutor();
		String statement =
				"INSERT INTO CHORDLESS_ENTRIES (ID, CONTENT, TIMESTAMP, VERSION, COMMUTATION, ITERATION, KLASS, LOCKEDBY) VALUES (?,?,?,?,?,?,?,?)";
		String locker = e.getLocker() == null ? null : e.getLocker().toString();
		int updatedRows = jdbc.execute(
				statement,
				e.getIdentifierString(),
				e.getBytes(),
				e.getTimestamp(),
				e.getVersion(),
				e.getCommutation(),
				e.getIteration(),
				e.getValueClassName(),
				locker
		);
		if (updatedRows != 1) {
			throw new RuntimeException(
					"'" + statement + "' with " + e + " did not update exactly one row, it updated " + updatedRows
			);
		}
	}

	private void sqlUpdate(Entry e, byte[] value) {
		ensurePersistExecutor();
		String statement =
				"UPDATE CHORDLESS_ENTRIES SET CONTENT = ?, TIMESTAMP = ?, VERSION = ?, COMMUTATION = ?, ITERATION= ?, KLASS = ?, LOCKEDBY = ? WHERE ID = ?";
		String locker = e.getLocker() == null ? null : e.getLocker().toString();
		int updatedRows = jdbc.execute(
				statement,
				value,
				e.getTimestamp(),
				e.getVersion(),
				e.getCommutation(),
				e.getIteration(),
				e.getValueClassName(),
				locker,
				e.getIdentifierString()
		);
		if (updatedRows != 1) {
			throw new RuntimeException(
					"'" + statement + "' with " + e + " did not update exactly one row, it updated " + updatedRows
			);
		}
	}

	public Map<Identifier, Entry> getEmpty() {
		ensurePersistExecutor();
		Map<Identifier, Entry> returnValue = new HashMap<Identifier, Entry>();
		ResultSetWrapper result = jdbc.query(
				"SELECT ID, TIMESTAMP, VERSION, COMMUTATION, ITERATION, KLASS, LOCKEDBY FROM CHORDLESS_ENTRIES"
		);
		while (result.next()) {
			Entry empty = resultToEmpty(result);
			returnValue.put(empty.getIdentifier(), empty);
		}
		return returnValue;
	}

	@Override
	public long youngestEntryAge() {
		ensurePersistExecutor();
		Long age = jdbc.query(Long.class, "TIMESTAMP",
				"SELECT TIMESTAMP FROM CHORDLESS_ENTRIES ORDER BY TIMESTAMP DESC LIMIT 1"
		);
		if (age == null) {
			return 0;
		} else {
			return age;
		}
	}

	@Override
	public long oldestEntryAge() {
		ensurePersistExecutor();
		Long age = jdbc.query(Long.class, "TIMESTAMP",
				"SELECT TIMESTAMP FROM CHORDLESS_ENTRIES ORDER BY TIMESTAMP ASC LIMIT 1"
		);
		if (age == null) {
			return 0;
		} else {
			return age;
		}
	}

	@Override
	public void destroy() {
		super.destroy();
		jdbc.execute("DELETE FROM CHORDLESS_ENTRIES");
		jdbc.execute("DELETE FROM CHORDLESS_MERKLE_NODES");
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void _put(Entry e) {

		SingletonEventBus.getEventBus().post(new PutEvent(e));

		//System.out.println("PUT " + e.getIdentifier() + "=" + Cerealizer.unpack(e.getBytes()));

		Entry oldEntry = getEmpty(e.getIdentifier());
		if (oldEntry == null) {

			//System.out.println("oldEntry is null, putting it");
			sqlPut(e);

		} else {

			//System.out.println("oldEntry is NOT null");
			e.bumpVersion(oldEntry);

			Object newContent = Cerealizer.unpack(e.getBytes());
			if (newContent instanceof ChordSet) {

				//System.out.println("newEntry is of type ChordSet (contents=" + newContent + ")");

				oldEntry = getInternal(e.getIdentifier());
				Object oldContent = Cerealizer.unpack(oldEntry.getBytes());
				if (oldContent instanceof ChordSet) {

					//System.out.println("oldEntry is of type ChordSet (contents=" + oldContent + ")");
					for (Object oldContentItem : (ChordSet) oldContent) {
						((ChordSet) newContent).add(oldContentItem);
					}
				}
			}

			//System.out.println("saving with content=" + newContent);
			sqlUpdate(e, Cerealizer.pack(newContent));
		}
	}

	@SuppressWarnings("unchecked")
	protected void _update(Entry oldEntry, Entry newEntry) {

		Entry currentEntry = getInternal(oldEntry.getIdentifier());

		UpdateEvent updateEvent;
		if (currentEntry == null) {

			put(newEntry);
			updateEvent = new UpdateEvent(newEntry.getIdentifier(), null, Cerealizer.unpack(newEntry.getBytes()));

		} else {

			Object currentContent = Cerealizer.unpack(currentEntry.getBytes());

			// Case 1: current entry contains a ChordSet
			if (currentContent instanceof ChordSet) {

				ChordSet currentSet = (ChordSet) currentContent;
				ChordSet entriesToBeRemoved = (ChordSet) Cerealizer.unpack(oldEntry.getBytes());
				ChordSet entriesToBeAdded = (ChordSet) Cerealizer.unpack(newEntry.getBytes());

				for (Object entry : entriesToBeRemoved) {
					currentSet.remove(entry);
				}

				for (Object entry : entriesToBeAdded) {
					currentSet.add(entry);
				}

				currentEntry.bumpVersion(currentEntry);
				sqlUpdate(currentEntry, Cerealizer.pack(currentSet));

				updateEvent = new UpdateEvent(newEntry.getIdentifier(), entriesToBeRemoved, entriesToBeAdded);
			}

			// Case 2: current entry doesn't contain a ChordSet --> overwrite it
			else {

				put(newEntry);
				updateEvent = new UpdateEvent(newEntry.getIdentifier(), currentContent,
						Cerealizer.unpack(newEntry.getBytes())
				);
			}
		}

		SingletonEventBus.getEventBus().post(updateEvent);
	}

	@Override
	protected void _commutePut(Entry e) {
		put(e);
	}

	public JDBC getJDBC() {
		return jdbc;
	}

	@Override
	public String[] getConstructorArguments() {
		return new String[]{getJDBC().getClassName(), getJDBC().getDbURL()};
	}

	private static MerkleNode resultToNode(ResultSetWrapper result) {
		String[] split = result.get(String.class, "ID").split(":");
		return new MerkleNode(
				new MerkleNode.ID(new Identifier(split[0]), new Identifier(split[1])),
				result.get(Integer.class, "ISLEAF") == 1,
				new Identifier(result.get(String.class, "HASH"))
		);
	}

	private static Entry resultToEmpty(ResultSetWrapper result) {
		return new Entry(
				result.get(String.class, "ID"),
				null,
				result.get(Long.class, "TIMESTAMP"),
				result.get(Long.class, "VERSION"),
				result.get(Long.class, "COMMUTATION"),
				result.get(Long.class, "ITERATION"),
				result.get(String.class, "KLASS"),
				result.get(String.class, "LOCKEDBY")
		);
	}

	private static Entry resultToEntry(ResultSetWrapper result) {
		return new Entry(result.get(String.class, "ID"),
				result.get(byte[].class, "CONTENT"),
				result.get(Long.class, "TIMESTAMP"),
				result.get(Long.class, "VERSION"),
				result.get(Long.class, "COMMUTATION"),
				result.get(Long.class, "ITERATION"),
				result.get(String.class, "KLASS"),
				result.get(String.class, "LOCKEDBY")
		);
	}

	@Override
	protected boolean _setLocker(Identifier identifier, Identifier holder) {
		ensurePersistExecutor();
		String statement = "UPDATE CHORDLESS_ENTRIES SET LOCKEDBY = ?, ITERATION = ITERATION + 1 WHERE ID = ?";
		int rows = jdbc.execute(statement, holder == null ? null : holder.toString(), identifier.toString());
		if (rows == 1) {
			return true;
		} else if (rows == 0) {
			return false;
		} else {
			throw new RuntimeException(
					"'" + statement + "' with " + holder + " and " + identifier + " updated more than one row!"
			);
		}
	}

	@Override
	public Entry get(Identifier k) {
		try {

			Entry entry = getInternal(k);
			SingletonEventBus.getEventBus().post(new ReadEvent(k, entry));
			return entry;

		} catch (NoSuchEntryException e) {

			SingletonEventBus.getEventBus().post(new ReadEvent(k, null));
			throw propagate(e);
		}
	}
	
	private Entry getInternal(Identifier k) {
		ensurePersistExecutor();
		String statement = "SELECT * FROM CHORDLESS_ENTRIES WHERE ID = ?";
		ResultSetWrapper result = jdbc.query(statement, k.toString());
		if (result.next()) {
			Entry returnValue = resultToEntry(result);
			if (result.next()) {
				throw new RuntimeException("'" + statement + "' with " + k + " returned more than one row");
			} else {
				return returnValue;
			}
		} else {
			throw new NoSuchEntryException(k, false);
		}
	}

	@Override
	protected Object _del(Identifier k, Entry e) {

		SingletonEventBus.getEventBus().post(new DeleteEvent(k, e));

		Entry oldEntry;
		try {
			oldEntry = getInternal(k);
		} catch (NoSuchEntryException e1) {
			return null;
		}

		Object currentEntry = Cerealizer.unpack(oldEntry.getBytes());
		Object valuesToDelete = e == null ? null : Cerealizer.unpack(e.getBytes());

		if (currentEntry instanceof ChordSet && valuesToDelete instanceof ChordSet) {

			for (Object valueToDelete : (ChordSet) valuesToDelete) {
				((ChordSet) currentEntry).remove(valueToDelete);
			}

			e.bumpVersion(e);
			sqlUpdate(e, Cerealizer.pack(currentEntry));
			return currentEntry;

		} else {

			ensurePersistExecutor();
			jdbc.execute("DELETE FROM CHORDLESS_ENTRIES WHERE ID = ?", k.toString());
			return null;
		}
	}

	@Override
	public Identifier nextIdentifier(Identifier identifier) {
		ensurePersistExecutor();
		String statement = "SELECT ID FROM CHORDLESS_ENTRIES WHERE ID > ? LIMIT 1";
		ResultSetWrapper result = jdbc.query(statement, identifier.toString());
		if (result.next()) {
			Identifier returnValue = new Identifier(result.get(String.class, "ID"));
			result.close();
			return returnValue;
		} else {
			statement = "SELECT ID FROM CHORDLESS_ENTRIES ORDER BY ID ASC LIMIT 1";
			result = jdbc.query(statement);
			if (result.next()) {
				Identifier returnValue = new Identifier(result.get(String.class, "ID"));
				result.close();
				return returnValue;
			} else {
				return null;
			}
		}
	}

	@Override
	public Entry getNextEntry(Identifier previous) {
		ensurePersistExecutor();
		String statement = "SELECT * FROM CHORDLESS_ENTRIES WHERE ID > ? LIMIT 1";
		ResultSetWrapper result = jdbc.query(statement, previous.toString());
		if (result.next()) {
			Entry returnValue = resultToEntry(result);
			if (result.next()) {
				throw new RuntimeException("'" + statement + "' with " + previous + " returned more than one row");
			} else {
				return returnValue;
			}
		} else {
			statement = "SELECT * FROM CHORDLESS_ENTRIES ORDER BY ID ASC LIMIT 1";
			result = jdbc.query(statement);
			if (result.next()) {
				Entry returnValue = resultToEntry(result);
				if (result.next()) {
					throw new RuntimeException("'" + statement + "' with " + previous + " returned more than one row");
				} else {
					return returnValue;
				}
			} else {
				return null;
			}
		}
	}

	@Override
	public void updateMerkleNode(MerkleNode node) {
		ensurePersistExecutor();
		String statement = "UPDATE CHORDLESS_MERKLE_NODES SET hash = ?, isLeaf = ? WHERE id = ?";
		int updatedRows = jdbc.execute(statement, node.getHash().toString(), new Integer(node.isLeaf() ? 1 : 0),
				node.id.toString()
		);
		if (updatedRows != 1) {
			throw new RuntimeException(
					"'" + statement + "' with " + node + " did not update exactly one row, it updated " + updatedRows
			);
		}
	}

	@Override
	public MerkleNode _getMerkleNode(MerkleNode.ID id) {
		ensurePersistExecutor();
		String statement = "SELECT * FROM CHORDLESS_MERKLE_NODES WHERE ID = ?";
		ResultSetWrapper result = jdbc.query(statement, id.toString());
		if (result.next()) {
			MerkleNode returnValue = resultToNode(result);
			if (result.next()) {
				throw new RuntimeException("'" + statement + "' with " + id + " returned more than one row");
			} else {
				return returnValue;
			}
		} else {
			return null;
		}
	}

	@Override
	protected void insertMerkleNode(MerkleNode node) {
		ensurePersistExecutor();
		MerkleNode oldNode = _getMerkleNode(node.id);
		if (oldNode != null) {
			warn(this, "Node with id " + node.id + " already exists! Deleting it now.");
			jdbc.execute("DELETE FROM CHORDLESS_MERKLE_NODES WHERE ID = ?", node.id.toString());
		}
		String statement = "INSERT INTO CHORDLESS_MERKLE_NODES (ID, PARENTID, HASH, ISLEAF) VALUES (?,?,?,?)";
		int updatedRows = jdbc.execute(statement, node.id.toString(),
				(node.id.getParent() == null ? null : node.id.getParent().toString()), node
				.getHash().toString(), new Integer(node.isLeaf() ? 1 : 0)
		);
		if (updatedRows != 1) {
			throw new RuntimeException(
					"'" + statement + "' with " + node + " did not update exactly one row, it updated " + updatedRows
			);
		}
	}

	@Override
	protected void deleteMerkleChildren(MerkleNode.ID parentId) {
		ensurePersistExecutor();
		jdbc.execute("DELETE FROM CHORDLESS_MERKLE_NODES WHERE PARENTID = ?", parentId.toString());
	}

	@Override
	public SortedSet<MerkleNode> _getMerkleChildren(MerkleNode.ID parentId) {
		ensurePersistExecutor();
		SortedSet<MerkleNode> returnValue = new TreeSet<MerkleNode>();
		ResultSetWrapper result =
				jdbc.query("SELECT * FROM CHORDLESS_MERKLE_NODES WHERE PARENTID = ?", parentId.toString());
		while (result.next()) {
			returnValue.add(resultToNode(result));
		}
		return returnValue;
	}

	private String criteriaFor(Identifier from, Identifier toAndIncluding) {
		int fromToTo = from.compareTo(toAndIncluding);
		if (fromToTo < 0) {
			return "ID > ? AND ID <= ?";
		} else if (fromToTo > 0) {
			return "(ID > ? OR ID <= ?)";
		} else {
			return "(ID > ? OR ID <= ?)";
		}
	}

	@Override
	protected int _count(Identifier min, Identifier max) {
		ensurePersistExecutor();
		ResultSetWrapper result =
				jdbc.query("SELECT COUNT(ID) AS \"count\" FROM CHORDLESS_ENTRIES WHERE " + criteriaFor(min, max),
						min.toString(),
						max.toString()
				);
		if (result.next()) {
			int returnValue = result.get(Number.class, "COUNT").intValue();
			if (result.next()) {
				throw new RuntimeException("WTH, counting entries gave more than one result?");
			} else {
				return returnValue;
			}
		} else {
			throw new RuntimeException("WTH, counting entries gave no result?");
		}
	}

	@Override
	public Entry getEmpty(Identifier k) {
		ensurePersistExecutor();
		String statement =
				"SELECT ID, TIMESTAMP, VERSION, COMMUTATION, ITERATION, KLASS, LOCKEDBY FROM CHORDLESS_ENTRIES WHERE ID = ?";
		ResultSetWrapper result = jdbc.query(statement, k.toString());
		if (result.next()) {
			Entry returnValue = resultToEmpty(result);
			if (result.next()) {
				throw new RuntimeException("'" + statement + "' with " + k + " returned more than one row");
			} else {
				return returnValue;
			}
		} else {
			return null;
		}
	}

	@Override
	protected Map<Identifier, Entry> getEmpty(Identifier from, Identifier toAndIncluding) {
		ensurePersistExecutor();
		Map<Identifier, Entry> returnValue = new HashMap<Identifier, Entry>();
		ResultSetWrapper result = jdbc.query(
				"SELECT ID, TIMESTAMP, VERSION, COMMUTATION, ITERATION, KLASS, LOCKEDBY FROM CHORDLESS_ENTRIES WHERE "
						+ criteriaFor(from, toAndIncluding), from.toString(), toAndIncluding.toString()
		);
		while (result.next()) {
			Entry empty = resultToEmpty(result);
			returnValue.put(empty.getIdentifier(), empty);
		}
		return returnValue;
	}

	private static class ChunkAndMax {

		public Map<Identifier, Entry> chunk;

		public Identifier max;

		public ChunkAndMax(ResultSetWrapper result) {
			max = null;
			chunk = new HashMap<Identifier, Entry>();
			while (result.next()) {
				Entry empty = resultToEmpty(result);
				chunk.put(empty.getIdentifier(), empty);
				if (max == null || max.compareTo(empty.getIdentifier()) < 0) {
					max = empty.getIdentifier();
				}
			}
		}
	}

	@Override
	protected void consumeEmpty(final String valueClassName, final Long maxTimestamp, final Identifier from,
								final Identifier toAndIncluding,
								final EntryMapConsumer consumer) {
		consumer.executor().execute(
				new FriendlyTask("" + consumer.getClass().getName() + ".consume(...) running since " + new Date()) {
					public int getPriority() {
						return consumer.getPriority();
					}

					public String getDescription() {
						return consumer.getDescription();
					}

					public void subrun() {
						ensurePersistExecutor();
						StringBuffer query = new StringBuffer(
								"SELECT ID, TIMESTAMP, VERSION, COMMUTATION, ITERATION, KLASS, LOCKEDBY FROM CHORDLESS_ENTRIES WHERE "
						);
						List<String> andParts = new LinkedList<String>();
						List<Object> parameters = new LinkedList<Object>();
						if (valueClassName != null) {
							andParts.add("KLASS = ?");
							parameters.add(valueClassName);
						}
						andParts.add("TIMESTAMP < ?");
						parameters.add(maxTimestamp);
						if (from != null && toAndIncluding != null) {
							andParts.add(criteriaFor(from, toAndIncluding));
							parameters.add(from.toString());
							parameters.add(toAndIncluding.toString());
						}
						Iterator<String> andIterator = andParts.iterator();
						while (andIterator.hasNext()) {
							query.append(andIterator.next());
							if (andIterator.hasNext()) {
								query.append(" AND ");
							}
						}
						query.append(" ORDER BY ID ASC LIMIT ?");
						parameters.add(consumer.limit());
						ResultSetWrapper result = JDBCStorage.this.jdbc.query(query.toString(), parameters.toArray());
						final ChunkAndMax chunk = new ChunkAndMax(result);
						if (consumer.valid(chunk.chunk)) {
							consumer.consume(chunk.chunk,
									new FriendlyTask("" + consumer.getClass()
											.getName() + ".restTask.run() running since " + new Date()
									) {
										public int getPriority() {
											return consumer.getPriority();
										}

										public String getDescription() {
											return consumer.getDescription();
										}

										public void subrun() {
											try {
												Identifier _from = from;
												Identifier _toAndIncluding = toAndIncluding;
												if (_from == null || _toAndIncluding == null) {
													_from = new Identifier(0);
													_toAndIncluding = Identifier.getMAX_IDENTIFIER();
												}
												if (_from.compareTo(_toAndIncluding) < 0) {
													JDBCStorage.this
															.consumeEmpty(valueClassName, maxTimestamp, chunk.max,
																	_toAndIncluding, consumer
															);
												} else {
													if (chunk.max.compareTo(_toAndIncluding) < 0) {
														JDBCStorage.this
																.consumeEmpty(valueClassName, maxTimestamp, _from,
																		chunk.max, consumer
																);
													} else {
														JDBCStorage.this
																.consumeEmpty(valueClassName, maxTimestamp, chunk.max,
																		Identifier.getMAX_IDENTIFIER(), consumer
																);
													}
												}
											} catch (Throwable t) {
												error(this, "Error trying to consume rest", t);
												throw new RuntimeException(t);
											}
										}
									}
							);
						}
					}
				}
		);
	}

	@Override
	public void close() {
		super.close();
		jdbc.close();
	}

	@Override
	protected <T> T transaction(Callable<T> callable) {
		return jdbc.transaction(callable);
	}
}
