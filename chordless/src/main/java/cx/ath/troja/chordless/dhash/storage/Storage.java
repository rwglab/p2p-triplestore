/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.storage;

import static cx.ath.troja.nja.Log.error;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.MerkleTree;
import cx.ath.troja.chordless.dhash.commands.EntryOfferCommand;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.ThreadState;

public abstract class Storage extends MerkleTree {

	public static class NotPersistExecutorException extends RuntimeException {
		public NotPersistExecutorException() {
			super("This operation must be executed in the persistExecutor, currently we are in " + ThreadState.get(ExecutorService.class));
		}
	}

	/**
	 * A consumer of bite sized entry maps.
	 */
	public interface EntryMapConsumer {
		/**
		 * Check if this consumer is valid with the given chunk.
		 * 
		 * If false is returned, no more entries will be processed by this consumer.
		 * 
		 * @param chunk
		 *            the chunk that may be consumed
		 * @return true if the consumer wants to run
		 */
		public boolean valid(Map<Identifier, Entry> chunk);

		/**
		 * The executor to use to consume the chunks.
		 * 
		 * @return the executor service
		 */
		public ExecutorService executor();

		/**
		 * The maximum number of entries to consume at once.
		 * 
		 * @return the bite size, or unlimited if -1
		 */
		public int limit();

		/**
		 * The priority to use when executing the chunks.
		 * 
		 * @return the priority
		 */
		public int getPriority();

		/**
		 * The description to use when executing the chunks.
		 * 
		 * @return the description
		 */
		public String getDescription();

		/**
		 * Consume a map of entries.
		 * 
		 * @param c
		 *            the bite to consume, will be empty when no more data can be fetched
		 * @param restTask
		 *            a runnable that will consume the rest of the entries using this consumer
		 */
		public void consume(Map<Identifier, Entry> c, Runnable restTask);
	}

	protected void ensurePersistExecutor() {
		if (!DHash.PERSIST_EXECUTOR.equals(ThreadState.get(ExecutorService.class))) {
			throw new NotPersistExecutorException();
		}
	}

	/**
	 * Delete everything from the storage.
	 * 
	 */
	public abstract void destroy();

	/**
	 * Get the timestamp of the youngest entry.
	 * 
	 * @return the timestamp of the youngest entry
	 */
	public long youngestEntryAge() {
		return 0;
	}

	/**
	 * Get the timestamp of the oldest entry.
	 * 
	 * @return the timestamp of the oldest entry
	 */
	public long oldestEntryAge() {
		return 0;
	}

	public int count() {
		return count(new Identifier(0), Identifier.getMAX_IDENTIFIER());
	}

	protected abstract void _update(Entry oldEntry, Entry newEntry);

	/**
	 * Insert an entry.
	 * 
	 * Will increment the version, iteration and commutation of that entry.
	 * 
	 * @param e
	 *            the entry to insert
	 */
	protected abstract void _put(Entry e);

	/**
	 * Insert an entry updated in a commutative way.
	 * 
	 * Will increment the iteration and commutation of that entry.
	 * 
	 * @param e
	 *            the entry to insert
	 */
	protected abstract void _commutePut(Entry e);

	private void putHelp(Entry e) {
		_put(e);
		merkleInsert(e.getIdentifier());
	}

	private void commutePutHelp(Entry e) {
		_commutePut(e);
		merkleInsert(e.getIdentifier());
	}

	private void updateHelp(Entry oldEntry, Entry newEntry) {
		_update(oldEntry, newEntry);
		merkleInsert(oldEntry.getIdentifier());
	}

	protected void update(final Entry oldEntry, final Entry newEntry) {
		transaction(new Callable<Object>() {
			public Object call() {
				updateHelp(oldEntry, newEntry);
				return null;
			}
		});
	}

	/**
	 * Used by the PutCommand to clearcache after commute
	 */
	protected void put(final Entry e) {
		transaction(new Callable<Object>() {
			public Object call() {
				putHelp(e);
				return null;
			}
		});
	}

	/**
	 * Used by the CommutePutCommand to avoid clearing cache after commute
	 */
	protected void commutePut(final Entry e) {
		transaction(new Callable<Object>() {
			public Object call() {
				commutePutHelp(e);
				return null;
			}
		});
	}

	/**
	 * Used by the ExecPutCommand to avoid clearing cache after commute
	 */
	protected void execPut(final Entry e) {
		transaction(new Callable<Object>() {
			public Object call() {
				putHelp(e);
				return null;
			}
		});
	}

	/**
	 * Used by the PutCommand to clear cache after put
	 */
	protected void put(final Collection<Entry> c) {
		transaction(new Callable<Object>() {
			public Object call() {
				for (Entry entry : c) {
					putHelp(entry);
				}
				return null;
			}
		});
	}

	/**
	 * Closes this storage.
	 * 
	 * Override if you need to.
	 */
	public void close() {
	}

	/**
	 * Execute the runnable within a transaction.
	 * 
	 * Override this if at all possible, to ensure both performance and integrity.
	 */
	protected <T> T transaction(Callable<T> callable) {
		try {
			return callable.call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get an identifier and timestamp from the storage.
	 * 
	 * The returned entry will not have its value set.
	 * 
	 * @param i
	 *            the identifier of the returned entry
	 * @return an Entry with only timestamp, identifier and valueClassName set, or null if none exists
	 */
	public abstract Entry getEmpty(Identifier i);

	/**
	 * Retrieve an entry.
	 * 
	 * @param k
	 *            the id of the entry to retrieve
	 * @return the entry, or throw a NoSuchEntryException
	 */
	public abstract Entry get(Identifier k);

	/**
	 * Delete an entry.
	 * 
	 * @param k
	 *            the id of the entry to delete
	 * @return whether there was an entry to delete
	 */
	protected abstract Object _del(Identifier k, Entry oldEntry);

	private boolean delHelp(Identifier i, Entry oldEntry) {
		Object currentContent = _del(i, oldEntry);
		if (currentContent == null) {
			merkleRemove(i);
		}
		return true;
	}

	/**
	 * Delete an entry and remove it from the object cache.
	 * 
	 * @param k
	 *            the id of the entry to delete
	 * @return whether there was an entry to delete
	 */
	protected boolean del(final Identifier i, final Entry oldEntry) {
		return transaction(new Callable<Boolean>() {
			public Boolean call() {
				return new Boolean(Storage.this.delHelp(i, oldEntry));
			}
		}).booleanValue();
	}

	protected void del(final Collection<Entry> c) {
		transaction(new Callable<Object>() {
			public Object call() {
				for (Entry entry : c) {
					Storage.this.delHelp(entry.getIdentifier(), null);
				}
				return null;
			}
		});
	}

	/**
	 * Get the first id above the given identifier.
	 * 
	 * If there is no entry with a bigger id, the first identifier in the storage is returned.
	 * 
	 * If the storage is empty, null is returned.
	 * 
	 * @param from
	 *            the identifier that we want the returned entry to be above
	 * @return the first entry with an id above the given identifier
	 */
	public abstract Identifier nextIdentifier(Identifier from);

	/**
	 * Consume identifiers and timestamps in the storage.
	 * 
	 * The consumed entries will not have their values set.
	 * 
	 * The from and toAndIncluding points may overlap 0, ie from may be greater than toAndIncluding, in which case all
	 * entries bigger than from OR smaller than or equal to toAndIncluding shall be returned.
	 * 
	 * This task has a few weird requirements. Look closely at the implementation in JDBCStorage if you want to
	 * reimplement!
	 * 
	 * If, and only if, both from and toAndIncluding are null, entries with any identifiers will be retrieved.
	 * 
	 * @param valueClassName
	 *            the valueClassName of the entries to retrieve, or null all entries should be matched
	 * @param maxTimestamp
	 *            only entries with timestamps less than this will be retrieved
	 * @param from
	 *            any identifiers above this will be retrieved
	 * @param toAndIncluding
	 *            any identifiers with equal to or below this will be retrieved
	 * @param consumer
	 *            that which will consume the produced entries
	 */
	protected abstract void consumeEmpty(String valueClassName, Long maxTimestamp, Identifier from, Identifier toAndIncluding,
			EntryMapConsumer consumer);

	public void consumeEmpty(String valueClassName, long maxTimestamp, EntryMapConsumer consumer) {
		consumeEmpty(valueClassName, maxTimestamp, null, null, consumer);
	}

	public void consumeEmpty(String valueClassName, Identifier from, Identifier toAndIncluding, EntryMapConsumer consumer) {
		consumeEmpty(valueClassName, new Long(System.currentTimeMillis()), from, toAndIncluding, consumer);
	}

	public void consumeEmpty(Identifier from, Identifier toAndIncluding, EntryMapConsumer consumer) {
		consumeEmpty(null, new Long(Long.MAX_VALUE), from, toAndIncluding, consumer);
	}

	/**
	 * Returns the next entry bigger than the given identifier.
	 * 
	 * @param previous
	 *            the Identifier we want the returnvalue to be after
	 * @return the next entry after the give identifier
	 */
	public abstract Entry getNextEntry(Identifier previous);

	public void gc(final DHash dhash) {
		consumeEmpty("null", System.currentTimeMillis() - Entry.NULL_ENTRY_LIFETIME, new EntryMapConsumer() {
			public String getDescription() {
				return Storage.class.getName() + ".gc";
			}

			public int getPriority() {
				return 10;
			}

			public ExecutorService executor() {
				return dhash.getPersistExecutor();
			}

			public int limit() {
				return EntryOfferCommand.MAX_DELIVERY_SIZE;
			}

			public boolean valid(Map<Identifier, Entry> chunk) {
				return true;
			}

			public void consume(final Map<Identifier, Entry> chunk, Runnable restTask) {
				try {
					if (dhash.getEnableGC()) {
						if (chunk.size() > 0) {
							Storage.this.del(chunk.values());
							dhash.getPersistExecutor().execute(restTask);
						} else {
							dhash.resetGC();
						}
					} else {
						dhash.resetGC();
					}
				} catch (Throwable t) {
					error(this, "Error while consuming gc", t);
					throw new RuntimeException(t);
				}
			}
		});
	}

}
