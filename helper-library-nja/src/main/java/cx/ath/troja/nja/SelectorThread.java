/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.warn;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Deque;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class SelectorThread extends Thread {

	private static SelectorThread instance = null;

	/**
	 * Get the SelectorThread of this JVM.
	 * 
	 * @return a singleton instance of SelectorThread that is already running
	 */
	public static SelectorThread getInstance() {
		if (instance == null) {
			instance = new SelectorThread();
		}
		return instance;
	}

	private Queue<Runnable> tasks;

	private Selector selector;

	private boolean run;

	private TimeKeeper timer;

	private int lastEvents;

	private SelectorThread() {
		try {
			setName("SelectorThread");
			setDaemon(true);
			selector = Selector.open();
			tasks = new ConcurrentLinkedQueue<Runnable>();
			timer = new TimeKeeper();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * How many events the last select returned.
	 * 
	 * @return the number of events
	 */
	public long getLastEvents() {
		return lastEvents;
	}

	/**
	 * Get the timekeeper for this thread.
	 * 
	 * @return the timekeeper
	 */
	public TimeKeeper getTimeKeeper() {
		return timer;
	}

	private synchronized void ensureRunning() {
		if (!isAlive()) {
			start();
		}
	}

	/**
	 * Set the interest ops for a selection key.
	 * 
	 * @param key
	 *            the key to set the ops for
	 * @param ops
	 *            the ops to set
	 * @return this
	 */
	protected SelectorThread interestOps(final SelectionKey key, final int ops) {
		tasks.add(new Runnable() {
			public void run() {
				key.interestOps(ops);
			}
		});
		wakeup();
		return this;
	}

	/**
	 * Register the given channel for OP_READ.
	 * 
	 * @param channel
	 *            the socket to register
	 * @return a future that will resolve to the selection key of the registration
	 */
	protected Future<SelectionKey> register(final SelectableChannel channel) {
		final Selector sel = selector;
		FutureTask<SelectionKey> returnValue = new FutureTask<SelectionKey>(new Callable<SelectionKey>() {
			public SelectionKey call() {
				try {
					return channel.register(sel, SelectionKey.OP_READ);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		tasks.add(returnValue);
		wakeup();
		ensureRunning();
		return returnValue;
	}

	/**
	 * Register the given socket for OP_ACCEPT.
	 * 
	 * @param serverSocket
	 *            the socket that is going to start accepting connections
	 * @return this
	 */
	protected SelectorThread register(SocketServer server) {
		try {
			final ServerSocketChannel serverSocket = server.getServerSocket();
			final Selector sel = selector;
			FutureTask<SelectionKey> futureKey = new FutureTask<SelectionKey>(new Callable<SelectionKey>() {
				public SelectionKey call() {
					try {
						return serverSocket.register(selector, SelectionKey.OP_ACCEPT);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
			tasks.add(futureKey);
			ensureRunning();
			wakeup();
			futureKey.get().attach(server);
			server.setSelectionKey(futureKey.get());
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Makes any select this thread is in the process of doing wakeup and restart.
	 */
	protected void wakeup() {
		selector.wakeup();
	}

	/**
	 * {@inheritDoc}
	 */
	public void run() {
		try {
			ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
			runTasks();
			while (selector.keys().size() > 0) {
				try {
					selector.select();
				} catch (CancelledKeyException e) {
					debug(this, "CancelledKeyException caught while doing Selector#select(). This is weird, and doesnt happen in Sun's java.", e);
				}
				timer.before();
				Set<SelectionKey> selectionKeys = selector.selectedKeys();
				lastEvents = selectionKeys.size();
				for (final SelectionKey selectionKey : selectionKeys) {
					try {
						if (selectionKey.isAcceptable()) {
							if (selectionKey.attachment() != null) {
								SocketServer server = (SocketServer) selectionKey.attachment();
								if (loggable(this, DEBUG))
									debug(this, "A connection for to " + server.getServerSocket());
								server.handleAccept(selectionKey);
							}
						}
						if (selectionKey.isWritable()) {
							if (selectionKey.attachment() != null) {
								handleWrite(selectionKey.channel(), (NetworkEventHandler) selectionKey.attachment(), selectionKey);
							}
						}
						if (selectionKey.isReadable()) {
							if (selectionKey.attachment() != null) {
								handleRead(selectionKey.channel(), (NetworkEventHandler) selectionKey.attachment(), buffer);
							}
						}
					} catch (CancelledKeyException e) {
						debug(this, "While checking possible operations on " + selectionKey.channel(), e);
						if (selectionKey.attachment() != null) {
							if (selectionKey.attachment() instanceof NetworkEventHandler) {
								((NetworkEventHandler) selectionKey.attachment()).close();
							} else if (selectionKey.attachment() instanceof SocketServer) {
								((SocketServer) selectionKey.attachment()).close();
							}
						}
					}
				}
				selectionKeys.clear();
				runTasks();
				timer.after();
			}
			instance = null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void handleWrite(SelectableChannel channel, NetworkEventHandler handler, SelectionKey selectionKey) {
		try {
			Deque<Object> writeQueue = handler.getWriteQueue();
			if (channel instanceof SocketChannel) {
				ByteBuffer toWrite = null;
				while ((toWrite = (ByteBuffer) writeQueue.poll()) != null && selectionKey.isWritable()) {
					((SocketChannel) channel).write(toWrite);
					if (toWrite.remaining() > 0) {
						writeQueue.addFirst(toWrite);
						break;
					}
				}
			} else if (channel instanceof DatagramChannel) {
				DatagramPacket packet = null;
				while ((packet = (DatagramPacket) writeQueue.poll()) != null && selectionKey.isWritable()) {
					if (((DatagramChannel) channel).send(ByteBuffer.wrap(packet.getData()), packet.getSocketAddress()) == 0) {
						writeQueue.addFirst(packet);
						break;
					}
				}
			} else {
				throw new RuntimeException("Unknown channel type selected " + channel);
			}
			if (writeQueue.size() == 0) {
				try {
					selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
				} catch (CancelledKeyException e) {
					debug(this, "While removing OP_WRITE on " + selectionKey, e);
					handler.close();
				}
			}
		} catch (IOException e) {
			if ("Connection reset by peer".equals(e.getMessage())) {
				debug(this, "While reading from " + channel, e);
			} else if ("Broken pipe".equals(e.getMessage())) {
				debug(this, "While reading from " + channel, e);
			} else {
				warn(this, "While reading from " + channel, e);
			}
			handler.close();
		}
	}

	private void handleReadForDatagrams(ByteBuffer buffer, NetworkEventHandler handler, DatagramChannel channel) throws IOException {
		SocketAddress source = channel.receive(buffer);
		int position = buffer.position();
		if (position > 0) {
			buffer.rewind();
			final byte[] bytes = new byte[position];
			buffer.get(bytes);
			try {
				handler.handleRead(bytes, source);
			} catch (Throwable t) {
				warn(this, "While handling read for " + handler, t);
			}
		}
	}

	private void handleReadForConnections(ByteBuffer buffer, NetworkEventHandler handler, SocketChannel channel) throws IOException {
		if (channel.read(buffer) == -1) {
			handler.close();
		} else {
			int position = buffer.position();
			buffer.rewind();
			final byte[] bytes = new byte[position];
			buffer.get(bytes);
			try {
				handler.handleRead(bytes, channel.socket().getRemoteSocketAddress());
			} catch (Throwable t) {
				warn(this, "While handling read for " + handler, t);
			}
		}
	}

	private void handleRead(SelectableChannel channel, NetworkEventHandler handler, ByteBuffer buffer) {
		buffer.rewind();
		try {
			if (channel instanceof SocketChannel) {
				handleReadForConnections(buffer, handler, (SocketChannel) channel);
			} else if (channel instanceof DatagramChannel) {
				handleReadForDatagrams(buffer, handler, (DatagramChannel) channel);
			} else {
				throw new RuntimeException("Unknown channel type " + channel);
			}
		} catch (CancelledKeyException e) {
			debug(this, "While reading from " + channel, e);
			handler.close();
		} catch (AsynchronousCloseException e) {
			debug(this, "While reading from " + channel, e);
			handler.close();
		} catch (ClosedChannelException e) {
			debug(this, "While reading from " + channel, e);
			handler.close();
		} catch (IOException e) {
			if ("Connection reset by peer".equals(e.getMessage())) {
				debug(this, "While reading from " + channel, e);
			} else {
				warn(this, "While reading from " + channel, e);
			}
			handler.close();
		}
	}

	private void runTasks() {
		Runnable task;
		while ((task = tasks.poll()) != null) {
			try {
				task.run();
			} catch (Exception e) {
				debug(this, "While trying to execute " + task, e);
			}
		}
	}

}
