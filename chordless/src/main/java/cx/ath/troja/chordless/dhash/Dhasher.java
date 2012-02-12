/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash;

import static cx.ath.troja.nja.Log.FINE;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.warn;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.commands.DHashCommand;
import cx.ath.troja.chordless.dhash.commands.DestroyAllCommand;
import cx.ath.troja.chordless.dhash.commands.ResetClassLoadersCommand;
import cx.ath.troja.chordless.dhash.commands.ReturnValueCommand;
import cx.ath.troja.nja.FutureValue;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.NamedRunnable;
import cx.ath.troja.nja.Proxist;

public class Dhasher extends DHashPersister implements ProxyProvider {

	public class ReturnValueCommandFuture<T> implements DelayedReceiver<T> {
		protected FutureValue<T> receiverFuture;

		private String pauseCause;

		public ReturnValueCommandFuture(String cause) {
			receiverFuture = new FutureValue<T>();
			pauseCause = cause;
		}

		@Override
		public String toString() {
			return "<" + this.getClass().getName() + " " + pauseCause + " future=" + receiverFuture + ">";
		}

		@Override
		public long getTimeout() {
			return Receiver.DEFAULT_TIMEOUT;
		}

		@Override
		public T get() {
			return get(Delay.DEFAULT_TIMEOUT);
		}

		@Override
		public T get(long timeout) {
			String oldName = Thread.currentThread().getName();
			try {
				Thread.currentThread().setName(oldName + " " + pauseCause);
				T t = receiverFuture.get(timeout, TimeUnit.MILLISECONDS);
				if (t instanceof RuntimeException) {
					if (loggable(this, FINE))
						debug(this, "Exception during " + pauseCause, (RuntimeException) t);
					RuntimeException exception;
					try {
						exception = (RuntimeException) t.getClass().getConstructor(String.class).newInstance(((RuntimeException) t).getMessage());
						exception.initCause((RuntimeException) t);
					} catch (NoSuchMethodException e) {
						try {
							exception = (RuntimeException) t.getClass().newInstance();
							exception.initCause((RuntimeException) t);
						} catch (Exception e2) {
							warn(this, "Failed to chain exception at Dhasher#get due to " + e2, (Exception) t);
							exception = (RuntimeException) t;
						}
					} catch (Exception e) {
						warn(this, "Failed to construct a new " + t.getClass() + " with " + ((RuntimeException) e).getMessage()
								+ " as argument due to " + e, (Exception) t);
						exception = (RuntimeException) t;
					}
					throw exception;
				} else {
					return t;
				}
			} catch (RuntimeException e) {
				throw e;
			} catch (java.util.concurrent.TimeoutException e) {
				throw new Delay.TimeoutException(e);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				Thread.currentThread().setName(oldName);
			}
		}

		@Override
		public void receive(ReturnValueCommand<T> command) {
			receiverFuture.set(command.getReturnValue());
		}
	}

	private DHash dhash;

	public Dhasher() {
	}

	public Dhasher(DHash c) {
		dhash = c;
	}

	public Dhasher setDHash(DHash d) {
		dhash = d;
		return this;
	}

	@Override
	public <T> DelayedReceiver<T> getReceiver(String job) {
		return new ReturnValueCommandFuture<T>(job);
	}

	protected DHash getDHash() {
		return dhash;
	}

	@Override
	protected ServerInfo getServerInfo() {
		return dhash.getServerInfo();
	}

	public Delay<Object> destroy() {
		DelayedReceiver<Object> returnValue = getReceiver("destroy()");
		register(new Identifier(0), new DestroyAllCommand(getServerInfo()), returnValue);
		return returnValue;
	}

	public Delay<Object> resetClassLoaders() {
		DelayedReceiver<Object> returnValue = getReceiver("resetClassLoaders()");
		register(new Identifier(0), new ResetClassLoadersCommand(getServerInfo()), returnValue);
		return returnValue;
	}

	@Override
	public void register(final Identifier identifier, final DHashCommand command, final Receiver receiver) {
		try {
			getDHash().getExecutorService().execute(
					new NamedRunnable("" + this + ".register(" + identifier + ", " + command + ", " + receiver + ") running since " + new Date()) {
						public void subrun() {
							Dhasher.this.getDHash().registerAndSendToSuccessor(identifier, command, receiver);
						}
					});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object find(Identifier identifier) {
		try {
			Delay<Class> classDelay = exec(identifier, "getClass");
			Class<Proxist.Proxy> proxyClass = Proxist.getInstance().proxyFor(classDelay.get());
			Proxist.Proxy proxy = proxyClass.newInstance();
			proxy.setForwarder(new ProxyProvider.Forwarder(this, identifier));
			return proxy;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
