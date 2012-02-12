/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.error;
import static cx.ath.troja.nja.Log.info;
import static cx.ath.troja.nja.Log.loggable;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.Receiver;
import cx.ath.troja.chordless.dhash.storage.Storage;
import cx.ath.troja.nja.Identifier;

public class CleanCommand extends DHashCommand {

	private static class OfferValidator implements EntryOfferCommand.OfferValidator {
		private Identifier uuid;

		public OfferValidator(Identifier u) {
			uuid = u;
		}

		public boolean validate(EntryOfferCommand command, DHash dhash) {
			if (command.getCaller().getIdentifier().equals(dhash.getIdentifier())) {
				return dhash.validateCleanOffer(uuid);
			} else {
				return true;
			}
		}
	}

	private ServerInfo[] successors;

	private Identifier identifier;

	public CleanCommand(ServerInfo c, Identifier i) {
		super(c);
		successors = null;
		identifier = i;
	}

	@Override
	protected Collection<Identifier> getRegarding() {
		Collection<Identifier> returnValue = new LinkedList<Identifier>();
		returnValue.add(identifier);
		if (successors != null && successors.length > 0 && successors[0] != null) {
			returnValue.add(successors[0].getIdentifier());
		}
		return returnValue;
	}

	@Override
	public int getPriority() {
		return 5;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " successors='" + (successors == null ? "null" : Arrays.asList(successors)) + "' identifier='"
				+ identifier + "' caller='" + caller + "' done='" + done + "' uuid='" + uuid + "'>";
	}

	protected void executeCleanup(final DHash dhash) {
		if (dhash.validateClean(this)) {
			dhash.getStorage().consumeEmpty(identifier.previous(), successors[0].getIdentifier(), new Storage.EntryMapConsumer() {
				public String getDescription() {
					return CleanCommand.class.getName() + ".executeCleanup";
				}

				public int getPriority() {
					return CleanCommand.this.getPriority();
				}

				public ExecutorService executor() {
					return CleanCommand.this.getExecutor(dhash);
				}

				public int limit() {
					return EntryOfferCommand.MAX_DELIVERY_SIZE;
				}

				public boolean valid(Map<Identifier, Entry> chunk) {
					return dhash.validateClean(CleanCommand.this);
				}

				public void consume(final Map<Identifier, Entry> chunk, final Runnable restTask) {
					try {
						if (chunk.size() > 0) {
							dhash.getStorage().del(chunk.values(), null, new Runnable() {
								public void run() {
									try {
										CleanCommand.this.getExecutor(dhash).execute(restTask);
									} catch (Throwable t) {
										error(this, "Error while trying to clean up", t);
										throw new RuntimeException(t);
									}
								}
							});
						} else {
							dhash.resetLastCleanCommand();
						}
					} catch (Throwable t) {
						error(this, "Error while trying to clean up", t);
						throw new RuntimeException(t);
					}
				}
			});

			if (loggable(this, DEBUG))
				debug(this, "" + this + " has executed cleanup!");
		}
	}

	private boolean nonNull(ServerInfo[] ary, int length) {
		for (int i = 0; i < length; i++) {
			if (ary[i] == null) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected void executeHome(final DHash dhash) {
		if (successors != null && dhash.validateClean(this)) {
			if (nonNull(successors, dhash.getCopies())) {
				if (dhash.getCopies() > 1
						&& caller.getIdentifier().betweenGTE_LTE(successors[0].getIdentifier(), successors[dhash.getCopies() - 1].getIdentifier())
						|| dhash.getCopies() == 1 && caller.getIdentifier().equals(successors[0].getIdentifier())) {

					dhash.resetLastCleanedIdentifier();
				} else {
					dhash.getStorage().consumeEmpty(identifier.previous(), successors[0].getIdentifier(), new Storage.EntryMapConsumer() {
						public ExecutorService executor() {
							return CleanCommand.this.getExecutor(dhash);
						}

						public int limit() {
							return EntryOfferCommand.MAX_DELIVERY_SIZE;
						}

						public String getDescription() {
							return CleanCommand.class.getName() + ".executeHome";
						}

						public int getPriority() {
							return CleanCommand.this.getPriority();
						}

						public boolean valid(Map<Identifier, Entry> surplus) {
							return dhash.validateClean(CleanCommand.this);
						}

						public void consume(final Map<Identifier, Entry> surplus, final Runnable restTask) {
							try {
								if (surplus.size() > 0) {
									Iterator<Entry> entryIterator = surplus.values().iterator();
									while (entryIterator.hasNext()) {
										Entry e = entryIterator.next();
										if (!e.fresh()) {
											entryIterator.remove();
										}
									}
									if (surplus.size() > 0) {
										final Set<EntryOfferCommand> offers = new HashSet<EntryOfferCommand>();
										for (int i = 0; i < dhash.getCopies(); i++) {
											if (successors[i] != null) {
												EntryOfferCommand offerCommand = new EntryOfferCommand(caller, successors[i], new ArrayList<Entry>(
														surplus.values()), new OfferValidator(getUUID()));
												offers.add(offerCommand);
											}
										}
										Iterator<EntryOfferCommand> iterator = offers.iterator();
										while (iterator.hasNext()) {
											EntryOfferCommand command = iterator.next();
											try {
												dhash.registerAndSend(command, command.getDestination().getAddress(),
														new Receiver<EntryOfferCommand>() {
															@Override
															public long getTimeout() {
																return DEFAULT_TIMEOUT;
															}

															@Override
															public void receive(EntryOfferCommand c) {
																offers.remove(c);
																if (offers.size() == 0) {
																	CleanCommand.this.getExecutor(dhash).execute(restTask);
																}
															}
														});
											} catch (ConnectException e) {
												info(this, "Error while trying to send " + command + " to " + command.getDestination(), e);
												iterator.remove();
											}
										}
									} else {
										CleanCommand.this.getExecutor(dhash).execute(restTask);
									}
								} else {
									CleanCommand.this.executeCleanup(dhash);
								}
							} catch (Throwable t) {
								error(this, "Error while trying to send offers", t);
								throw new RuntimeException(t);
							}
						}
					});
				}
			}
		}
	}

	@Override
	protected void executeAway(DHash dhash) {
		if (dhash.getLastTopologyChange() < getCreatedAt()) {
			ServerInfo[] localSuccessors = dhash.getSuccessorArray();
			successors = new ServerInfo[localSuccessors.length];
			System.arraycopy(localSuccessors, 0, successors, 1, localSuccessors.length - 1);
			successors[0] = dhash.getServerInfo();
		}
		returnHome(dhash);
	}

}
