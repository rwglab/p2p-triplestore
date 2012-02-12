/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.commands;

import static cx.ath.troja.nja.Log.warn;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.storage.NoSuchEntryException;
import cx.ath.troja.nja.Identifier;

public class EntryOfferCommand extends Command {

	public final static int MAX_DELIVERY_SIZE = 8;

	protected Collection<Entry> offer;

	protected Collection<Entry> delivery;

	protected Collection<Entry> request;

	protected ServerInfo destination;

	protected boolean onDelivery;

	protected OfferValidator validator;

	public interface OfferValidator extends Serializable {
		public boolean validate(EntryOfferCommand command, DHash dhash);
	}

	public EntryOfferCommand(ServerInfo c, ServerInfo d, Collection<Entry> s, OfferValidator v) {
		super(c);
		destination = d;
		offer = s;
		onDelivery = true;
		delivery = new LinkedList<Entry>();
		request = new LinkedList<Entry>();
		validator = v;
	}

	@Override
	protected Collection<Identifier> getRegarding() {
		Collection<Identifier> returnValue = new LinkedList<Identifier>();
		Collection<Entry> source = null;
		if (delivery.size() > 0) {
			source = delivery;
		} else if (request.size() > 0) {
			source = request;
		} else {
			source = offer;
		}
		for (Entry entry : source) {
			returnValue.add(entry.getIdentifier());
		}
		return returnValue;
	}

	@Override
	public int getPriority() {
		return 5;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getName() + " offer='" + offer + "' delivery='" + delivery + "' destination='" + destination + "' onDelivery='"
				+ onDelivery + "' caller='" + caller + "' uuid='" + uuid + "'>";
	}

	public ServerInfo getDestination() {
		return destination;
	}

	public ServerInfo getCaller() {
		return caller;
	}

	protected void clearDelivery() {
		delivery.clear();
	}

	protected void request(Sender sender) {
		onDelivery = false;
		sender.send(this);
	}

	@Override
	public void execute(Chord chord, final Sender sender) {
		DHash dhash = (DHash) chord;
		if (validator.validate(this, dhash)) {
			if (onDelivery) {
				if (offer.size() > 0) {
					Iterator<Entry> offerIterator = offer.iterator();
					while (request.size() < MAX_DELIVERY_SIZE && offerIterator.hasNext()) {
						Entry offeredEmptyEntry = offerIterator.next();
						Entry alreadyHeldEmptyEntry = dhash.getStorage().getEmpty(offeredEmptyEntry.getIdentifier());
						if (alreadyHeldEmptyEntry == null || alreadyHeldEmptyEntry.getIteration() < offeredEmptyEntry.getIteration()) {
							request.add(offeredEmptyEntry);
						}
						offerIterator.remove();
					}
				}
				if (delivery.size() > 0) {
					dhash.getStorage().put(delivery, null, new Runnable() {
						public void run() {
							EntryOfferCommand.this.clearDelivery();
							EntryOfferCommand.this.request(sender);
						}
					});
				} else {
					request(sender);
				}
			} else {
				if (delivery.size() > 0) {
					warn(this, "" + this + " is not on delivery, but has a delivery with size " + delivery.size());
				} else {
					if (request.size() > 0) {
						Iterator<Entry> requestIterator = request.iterator();
						while (delivery.size() < MAX_DELIVERY_SIZE && requestIterator.hasNext()) {
							try {
								Entry deliveryEntry = dhash.getStorage().get(requestIterator.next().getIdentifier());
								delivery.add(deliveryEntry);
							} catch (NoSuchEntryException e) {
							}
							requestIterator.remove();
						}
					} else {
						if (offer.size() > 0) {
							warn(this, "" + this + " is not on delivery, and has no request, but has an offer of size " + offer.size());
						}
						dhash.deliver(this);
						/**
						 * Stop bouncing!
						 */
						return;
					}
				}
				onDelivery = true;
				sender.send(this);
			}
		}
	}

	@Override
	protected ExecutorService getExecutor(Chord chord) {
		return ((DHash) chord).getPersistExecutor();
	}

}
