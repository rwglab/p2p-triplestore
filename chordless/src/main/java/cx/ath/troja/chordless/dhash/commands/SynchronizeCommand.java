/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import static cx.ath.troja.nja.Log.TRACE;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.trace;
import static cx.ath.troja.nja.Log.warn;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.MerkleNode;
import cx.ath.troja.chordless.dhash.Receiver;
import cx.ath.troja.chordless.dhash.storage.Storage;
import cx.ath.troja.nja.Identifier;

public class SynchronizeCommand extends DHashCommand {

	private static class OfferValidator implements EntryOfferCommand.OfferValidator {
		private long validAt;

		private Identifier initiator;

		public OfferValidator(Identifier i, long v) {
			validAt = v;
			initiator = i;
		}

		public boolean validate(EntryOfferCommand command, DHash dhash) {
			if (dhash.getIdentifier().equals(initiator)) {
				return dhash.validateSynchronize(validAt);
			} else {
				return true;
			}
		}
	}

	private Identifier from;

	private Identifier toAndIncluding;

	private MerkleNode homeNode;

	private MerkleNode awayNode;

	private ServerInfo destination;

	private long validAt;

	public SynchronizeCommand(ServerInfo c, ServerInfo d, Identifier f, Identifier t, MerkleNode l, long v) {
		super(c);
		destination = d;
		from = f;
		toAndIncluding = t;
		homeNode = l;
		awayNode = null;
		validAt = v;
	}

	public long getValidAt() {
		return validAt;
	}

	@Override
	protected Collection<Identifier> getRegarding() {
		Collection<Identifier> returnValue = new LinkedList<Identifier>();
		returnValue.add(from);
		returnValue.add(toAndIncluding);
		return returnValue;
	}

	@Override
	public int getPriority() {
		return 5;
	}

	@Override
	public String toString() {
		return "<" + getClass().getName() + " caller='" + caller.getIdentifier().toString() + "' destination='"
				+ destination.getIdentifier().toString() + "' from='" + from.toString() + "' toAndIncluding='" + toAndIncluding.toString()
				+ "' homeNode='" + homeNode + "' awayNode='" + awayNode + "'>";
	}

	protected ServerInfo otherEndTo(ServerInfo info) {
		if (info.getIdentifier().equals(caller.getIdentifier())) {
			return destination;
		} else if (info.getIdentifier().equals(destination.getIdentifier())) {
			return caller;
		} else {
			throw new RuntimeException("" + info + " is neither caller nor destination?");
		}
	}

	private boolean interesting(Entry e) {
		return e.fresh() && e.getIdentifier().betweenGT_LTE(from, toAndIncluding);
	}

	private void sendOffer(final DHash dhash, final Collection<Entry> offer, Receiver<EntryOfferCommand> receiver) {
		if (offer.size() > 0) {
			final long synchValidAt = getValidAt();
			EntryOfferCommand command = new EntryOfferCommand(dhash.getServerInfo(), SynchronizeCommand.this.otherEndTo(dhash.getServerInfo()),
					offer, new OfferValidator(caller.getIdentifier(), getValidAt()));
			ServerInfo d = SynchronizeCommand.this.otherEndTo(dhash.getServerInfo());
			try {
				if (receiver == null) {
					dhash.send(command, d.getAddress());
				} else {
					dhash.registerAndSend(command, d.getAddress(), receiver);
				}
			} catch (ConnectException e) {
				warn(this, "Error while trying to send " + command + " to " + d);
			}
		}
	}

	private void offerInterval(final DHash dhash, Identifier start, Identifier end) {
		if (loggable(this, TRACE)) {
			trace(this, "" + dhash + " going to offer " + start + "-" + end + " to " + otherEndTo(dhash.getServerInfo()));
		}
		dhash.getStorage().consumeEmpty(start, end, new Storage.EntryMapConsumer() {
			public int getPriority() {
				return SynchronizeCommand.this.getPriority();
			}

			public String getDescription() {
				return SynchronizeCommand.class.getName() + ".offerInterval";
			}

			public ExecutorService executor() {
				return SynchronizeCommand.this.getExecutor(dhash);
			}

			public int limit() {
				return EntryOfferCommand.MAX_DELIVERY_SIZE;
			}

			public boolean valid(Map<Identifier, Entry> map) {
				if (dhash.getIdentifier().equals(caller.getIdentifier())) {
					return dhash.validateSynchronize(getValidAt());
				} else {
					return dhash.getLastTopologyChange() < SynchronizeCommand.this.getValidAt();
				}
			}

			public void consume(Map<Identifier, Entry> map, final Runnable restTask) {
				if (map.size() > 0) {
					Iterator<Map.Entry<Identifier, Entry>> iterator = map.entrySet().iterator();
					while (iterator.hasNext()) {
						if (!interesting(iterator.next().getValue())) {
							iterator.remove();
						}
					}
					if (map.size() > 0) {
						sendOffer(dhash, new ArrayList<Entry>(map.values()), new Receiver<EntryOfferCommand>() {
							@Override
							public long getTimeout() {
								return DEFAULT_TIMEOUT;
							}

							@Override
							public void receive(EntryOfferCommand c) {
								SynchronizeCommand.this.getExecutor(dhash).execute(restTask);
							}
						});
					} else {
						SynchronizeCommand.this.getExecutor(dhash).execute(restTask);
					}
				}
			}
		});
	}

	private void compareNodes(MerkleNode remote, MerkleNode local, final DHash dhash) {
		Collection<Entry> offer = new LinkedList<Entry>();
		if (local.isLeaf() && remote.isLeaf()) {
			for (Entry leaf : local.getLeafs().values()) {
				if (interesting(leaf)) {
					Entry remoteEntry = remote.getLeafs().get(leaf.getIdentifier());
					if (remoteEntry == null || remoteEntry.getIteration() < leaf.getIteration()) {
						offer.add(leaf);
					}
				}
			}
			sendOffer(dhash, offer, null);
		} else if (remote.isLeaf()) {
			if (from.compareTo(toAndIncluding) < 0) {
				offerInterval(dhash, from.max(local.id.min.previous()), toAndIncluding.min(local.id.max));
			} else {
				if (local.id.min.compareTo(toAndIncluding) < 1) {
					offerInterval(dhash, local.id.min.previous(), toAndIncluding.min(local.id.max));
				}
				if (local.id.max.compareTo(from) > -1) {
					offerInterval(dhash, from.max(local.id.min), local.id.max);
				}
			}
		}
	}

	@Override
	protected void executeHome(DHash dhash) {
		if (dhash.validateSynchronize(getValidAt())) {
			compareNodes(awayNode, homeNode, dhash);
			if (!awayNode.isLeaf() && !homeNode.isLeaf()) {
				for (MerkleNode child : dhash.getStorage().getMerkleChildren(homeNode.id)) {
					if (from.betweenGTE_LT(child.id.min, child.id.max) || toAndIncluding.betweenGTE_LTE(child.id.min, child.id.max)
							|| child.id.min.betweenGT_LTE(from, toAndIncluding) || child.id.max.betweenGT_LTE(from, toAndIncluding)) {
						SynchronizeCommand command = new SynchronizeCommand(caller, destination, from, toAndIncluding, child, validAt);
						try {
							dhash.send(command, destination.getAddress());
						} catch (ConnectException e) {
							warn(this, "Error while trying to send " + command + " to " + destination, e);
						}
					}
				}
			}
		}
	}

	@Override
	protected void executeAway(DHash dhash) {
		if (dhash.getLastTopologyChange() < validAt) {
			awayNode = dhash.getStorage().getMerkleNode(homeNode.id);
			if (!awayNode.getHash().equals(homeNode.getHash())) {
				compareNodes(homeNode, awayNode, dhash);
				returnHome(dhash);
			}
		}
	}

}
