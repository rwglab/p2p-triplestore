/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.storage;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.TRACE;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.info;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.trace;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.chordless.dhash.ExecDhasher;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Receiver;
import cx.ath.troja.chordless.dhash.commands.ClassLoadCommand;
import cx.ath.troja.chordless.dhash.commands.EnvoyCommand;
import cx.ath.troja.chordless.dhash.commands.ExecCommand;
import cx.ath.troja.chordless.dhash.transactions.TransactionBackend;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.ConcurrentHashSet;
import cx.ath.troja.nja.DynamicClassLoader;
import cx.ath.troja.nja.FriendlyTask;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Log;
import cx.ath.troja.nja.NamedRunnable;
import cx.ath.troja.nja.Proxist;
import cx.ath.troja.nja.ReferenceMap;
import cx.ath.troja.nja.ThreadState;
import cx.ath.troja.nja.TimeKeeper;

public abstract class ExecStorage extends Storage {

	public interface ByteProducer {
		public byte[] getBytes();
	}

	public static Object exec(Object object, Method method, Object... arguments) {
		Object returnValue;
		try {
			returnValue = method.invoke(object, arguments);
		} catch (RuntimeException e) {
			returnValue = e;
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof RuntimeException) {
				returnValue = e.getCause();
			} else {
				returnValue = new RuntimeException(e.getCause());
			}
		} catch (Exception e) {
			returnValue = new RuntimeException(e.getCause());
		}
		return returnValue;
	}

	protected Map<Identifier, Object> valueById = new ReferenceMap<Identifier, Object>();

	private Map<String, Set<Runnable>> unresolvedCalls = new HashMap<String, Set<Runnable>>();

	private DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

	public void _break_by_forgetting(Identifier i) {
		eraseCache(i);
	}

	public DynamicClassLoader getClassLoader() {
		return classLoader;
	}

	public void resetClassLoader() {
		synchronized (valueById) {
			valueById.clear();
			classLoader = new DynamicClassLoader(getClass().getClassLoader());
		}
	}

	@Override
	public void destroy() {
		synchronized (valueById) {
			valueById = new ReferenceMap<Identifier, Object>();
			unresolvedCalls = new HashMap<String, Set<Runnable>>();
			classLoader = new DynamicClassLoader(getClass().getClassLoader());
		}
	}

	private boolean inPersistThread() {
		return ThreadState.get(ExecutorService.class).equals(DHash.PERSIST_EXECUTOR);
	}

	public void envoy(final DHash dhash, final EnvoyCommand command) {
		try {
			final Object object = getObject(dhash, command.getIdentifier());
			String description = command.getEnvoy(getClassLoader()).getClass().getName() + ".handle(...)";
			final Runnable execution = new Runnable() {
				public void run() {
					String profileKey = "cx.ath.troja.chordless.dhash.commands.EnvoyCommand resulting execution";
					dhash.getExecutorService().getTimeKeeper().begin(profileKey);
					dhash.getExecutorService().getTimeKeeper().begin(command.getEnvoy(getClassLoader()).getClass().getName());

					ExecDhasher persister = new ExecDhasher(dhash, command.getClassLoaderHost());
					Persistent.setPersister(persister, object);

					TaintAware.State taintState = TaintAware.StateProducer.get(object);

					if (object instanceof TransactionBackend) {
						synchronized (object) {
							command.handle(dhash, object, getClassLoader());
						}
					} else {
						command.handle(dhash, object, getClassLoader());
					}

					if (!dhash.isShutdown()) {
						command.done(persister, object, taintState.tainted());
						dhash.getExecutorService().getTimeKeeper().end(profileKey);
						dhash.getExecutorService().getTimeKeeper().end(command.getEnvoy(getClassLoader()).getClass().getName());
						Persistent.clearPersister(object);
					}
				}
			};
			if (inPersistThread()) {
				dhash.getExecExecutor().execute(new NamedRunnable(description + " running since " + new Date()) {
					public void subrun() {
						execution.run();
					}
				});
			} else {
				execution.run();
			}
		} catch (NotPersistExecutorException e) {
			dhash.getPersistExecutor().execute(new FriendlyTask("" + this + ".envoy(" + dhash + ", " + command + ") running since " + new Date()) {
				public String getDescription() {
					return command.getClass().getName() + ".run";
				}

				public int getPriority() {
					return command.getPriority();
				}

				public void subrun() {
					try {
						ExecStorage.this.envoy(dhash, command);
					} catch (Throwable t) {
						Log.error(this, "Error during " + ExecStorage.this + ".envoy(" + dhash + ", " + command + ")", t);
					}
				}
			});
		} catch (final Cerealizer.ClassNotFoundException e) {
			requestClass(dhash, command.getClassLoaderHost(), e.getClassName(), new Runnable() {
				public void run() {
					ExecStorage.this.envoy(dhash, command);
				}
			});
		} catch (RuntimeException e) {
			command.handleException(dhash, e);
		} catch (Exception e) {
			command.handleException(dhash, new RuntimeException(e));
		}
	}

	public void exec(final DHash dhash, final ExecCommand command) {
		try {
			final Object object = getObject(dhash, command.getIdentifier());
			final Object[] arguments = command.getArguments(getClassLoader());
			final Method method = Proxist.getMethod(object, command.getMethodName(), arguments);
			Object sync = (object instanceof TransactionBackend) ? object : null;
			final Runnable execution = new Runnable() {
				public void run() {
					String profileKey = "cx.ath.troja.chordless.dhash.commands.ExecCommand resulting execution";
					dhash.getExecutorService().getTimeKeeper().begin(profileKey);
					dhash.getExecutorService().getTimeKeeper().begin(method.toString());

					ExecDhasher persister = new ExecDhasher(dhash, command.getClassLoaderHost());
					Persistent.setPersister(persister, object);

					TaintAware.State taintState = TaintAware.StateProducer.get(object);

					Object returnValue = null;
					if (object instanceof TransactionBackend) {
						synchronized (object) {
							returnValue = exec(object, method, arguments);
						}
					} else {
						returnValue = exec(object, method, arguments);
					}

					if (!dhash.isShutdown()) {
						command.done(dhash, object, returnValue, taintState.tainted(), persister);
						dhash.getExecutorService().getTimeKeeper().end(profileKey);
						dhash.getExecutorService().getTimeKeeper().end(method.toString());
						Persistent.clearPersister(object);
					}
				}
			};
			if (inPersistThread()) {
				dhash.getExecExecutor().execute(new NamedRunnable("" + method + " running since " + new Date()) {
					public void subrun() {
						execution.run();
					}
				});
			} else {
				execution.run();
			}
		} catch (NotPersistExecutorException e) {
			dhash.getPersistExecutor().execute(new FriendlyTask("" + this + ".exec(" + dhash + ", " + command + ") running since " + new Date()) {
				public String getDescription() {
					return command.getClass().getName() + ".run";
				}

				public int getPriority() {
					return command.getPriority();
				}

				public void subrun() {
					try {
						ExecStorage.this.exec(dhash, command);
					} catch (Throwable t) {
						Log.error(this, "Error during " + ExecStorage.this + ".exec(" + dhash + ", " + command + ")", t);
					}
				}
			});
		} catch (final Cerealizer.ClassNotFoundException e) {
			requestClass(dhash, command.getClassLoaderHost(), e.getClassName(), new Runnable() {
				public void run() {
					exec(dhash, command);
				}
			});
		} catch (RuntimeException e) {
			command.handleException(dhash, e);
		} catch (Exception e) {
			command.handleException(dhash, new RuntimeException(e));
		}
	}

	public Object getObject(DHash dhash, Identifier identifier) {
		TimeKeeper.Occurence work = new TimeKeeper.Occurence();
		work.begin(0);
		Object returnValue = null;

		// eraseCache(identifier);

		if ((returnValue = TransactionBackend.getBackend(identifier)) == null) {
			synchronized (valueById) {
				if ((returnValue = valueById.get(identifier)) == null) {
					ensurePersistExecutor();
					Entry entry = get(identifier);
					if ((returnValue = Cerealizer.unpack(entry.getBytes(), classLoader)) == null) {
						throw new NoSuchEntryException(identifier, true);
					} else {
						valueById.put(identifier, returnValue);
						work.end();
						dhash.getExecutorService().getTimeKeeper().merge("cache miss", work);
					}
				} else {
					work.end();
					dhash.getExecutorService().getTimeKeeper().merge("cache hit", work);
				}
			}
		}
		return returnValue;
	}

	private void addClass(String className, byte[] data) {
		classLoader.addClass(className, data);
	}

	private void resolveMissing(String className) {
		Set<Runnable> unresolved = unresolvedCalls.get(className);
		Iterator<Runnable> iterator = unresolved.iterator();
		try {
			while (iterator.hasNext()) {
				Runnable r = iterator.next();
				if (loggable(this, TRACE))
					trace(this, "Running waiting " + r + " after having resolved " + className);
				r.run();
				iterator.remove();
			}
		} finally {
			if (unresolved.size() == 0) {
				unresolvedCalls.remove(className);
			}
		}
	}

	private void removeUnresolved(String className) {
		unresolvedCalls.remove(className);
	}

	private boolean addUnresolved(Runnable runnable, String className) {
		boolean unknownName = false;
		Set<Runnable> unresolved = unresolvedCalls.get(className);
		if (unresolved == null) {
			unknownName = true;
			unresolved = new ConcurrentHashSet<Runnable>();
			unresolvedCalls.put(className, unresolved);
		}
		unresolved.add(runnable);
		return unknownName;
	}

	private void replaceAndRequest(DHash dhash, ServerInfo classLoaderHost, String from, String to) {
		Set<Runnable> unresolved = unresolvedCalls.remove(from);
		if (unresolved == null) {
			throw new RuntimeException(from + " is not known to be unresolved!");
		} else {
			unresolvedCalls.put(to, unresolved);
		}
		requestClass(dhash, classLoaderHost, to);
	}

	private void requestClass(final DHash dhash, final ServerInfo classLoaderHost, final String className) {
		ClassLoadCommand command = new ClassLoadCommand(dhash.getServerInfo(), classLoaderHost, className);
		try {
			dhash.registerAndSend(command, classLoaderHost.getAddress(), new Receiver<ClassLoadCommand>() {
				public long getTimeout() {
					return DEFAULT_TIMEOUT;
				}

				private void stillMissing(String newMissing, NoClassDefFoundError e) {
					if (newMissing.equals(className)) {
						throw new RuntimeException("Supplied bytecode for " + className + " doesnt seem to work properly", e);
					} else {
						ExecStorage.this.replaceAndRequest(dhash, classLoaderHost, className, newMissing);
					}
				}

				private void handle(ClassLoadCommand c, NoClassDefFoundError e) {
					if (loggable(this, DEBUG))
						debug(this, "While trying to load from " + c, e);
					if (e.getCause() != null && e.getCause() instanceof ClassNotFoundException) {
						stillMissing(e.getCause().getMessage(), e);
					} else {
						throw new RuntimeException("Got " + e + " with unknown cause", e);
					}
				}

				public void receive(ClassLoadCommand c) {
					boolean success = false;
					try {
						ExecStorage.this.addClass(className, c.getReturnValue());
						success = true;
						if (loggable(this, DEBUG))
							debug(this, "Successfully loaded from " + c);
					} catch (Cerealizer.ClassNotFoundException e) {
						if (e.getCause() != null && e.getCause() instanceof NoClassDefFoundError) {
							handle(c, (NoClassDefFoundError) e.getCause());
						} else {
							throw new RuntimeException("Got " + e + " with unknown cause", e);
						}
					} catch (NoClassDefFoundError e) {
						handle(c, e);
					}
					if (success) {
						ExecStorage.this.resolveMissing(className);
					}
				}
			});
		} catch (ConnectException e) {
			removeUnresolved(className);
			info(this, "Error while trying to send " + command + " to " + classLoaderHost.getAddress(), e);
		}
	}

	public void requestClass(DHash dhash, ServerInfo classLoaderHost, String className, Runnable runnable) {
		if (addUnresolved(runnable, className)) {
			requestClass(dhash, classLoaderHost, className);
		} else if (loggable(this, DEBUG)) {
			debug(this, "" + this + " is not sending any ClassLoadCommands for " + className
					+ " since the class was already known to be missing (and should be arriving forthwith)");
		}
	}

	protected void eraseCache(Identifier i) {
		synchronized (valueById) {
			valueById.remove(i);
		}
	}

	protected void updateCache(Entry e) {
		synchronized (valueById) {
			if (e.getValueClassName().equals("null")) {
				eraseCache(e.getIdentifier());
			} else {
				valueById.put(e.getIdentifier(), e.getValue(getClassLoader()));
			}
		}
	}

	@Override
	protected void put(Entry e) {
		super.put(e);
		eraseCache(e.getIdentifier());
	}

	protected void update(Entry oldEntry, Entry newEntry) {
		super.update(oldEntry, newEntry);
		eraseCache(newEntry.getIdentifier());
	}

	@Override
	protected void put(Collection<Entry> c) {
		super.put(c);
		for (Entry entry : c) {
			eraseCache(entry.getIdentifier());
		}
	}

	@Override
	protected void del(Collection<Entry> c) {
		super.del(c);
		for (Entry entry : c) {
			eraseCache(entry.getIdentifier());
		}
	}

	@Override
	protected boolean del(Identifier i, Entry oldEntry) {
		boolean returnValue = super.del(i, oldEntry);
		eraseCache(i);
		return returnValue;
	}

}
