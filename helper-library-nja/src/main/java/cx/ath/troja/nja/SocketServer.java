/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.warn;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class that makes it easy (omg) to use java.nio for creating a socket server.
 */
public class SocketServer<T extends SocketServerEventHandler> {

	public static final int LOOKUP_PORT = 7571;

	public static final String LOOKUP_ADDRESS = "224.0.75.71";

	public interface LookupResponder {
		public Object response(SocketAddress address);
	}

	private static class DefaultLookupResponder implements LookupResponder {
		public Object response(SocketAddress address) {
			return address;
		}
	}

	private class MulticastThread extends Thread {
		private MulticastSocket socket;

		private String serviceName;

		private LookupResponder responder;

		private boolean running = true;

		public MulticastThread(String s, LookupResponder r) {
			super("SocketServer.multicastThread");
			try {
				setDaemon(true);
				socket = new MulticastSocket(LOOKUP_PORT);
				socket.setSoTimeout(1000);
				socket.setReuseAddress(true);
				socket.joinGroup(InetAddress.getByName(LOOKUP_ADDRESS));
				socket.setTimeToLive(32);
				responder = r;
				serviceName = s;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public void kill() {
			running = false;
		}

		public void run() {
			try {
				while (running) {
					byte[] buffer = new byte[1024];
					DatagramPacket message = new DatagramPacket(buffer, buffer.length);
					try {
						socket.receive(message);
						if (running) {
							String messageService = new String(message.getData(), 0, message.getLength());
							if (messageService.equals(serviceName)) {
								Object response = responder.response(SocketServer.this.getServerSocket().socket().getLocalSocketAddress());
								if (response != null) {
									byte[] bytes = Cerealizer.pack(response);
									DatagramPacket reply = new DatagramPacket(bytes, bytes.length, message.getSocketAddress());
									DatagramSocket replySocket = new DatagramSocket();
									replySocket.send(reply);
									replySocket.close();
								}
							}
						}
					} catch (SocketTimeoutException e) {
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static DatagramSocket sendServiceQuery(String name) throws IOException {
		DatagramSocket socket = new DatagramSocket();
		byte[] buffer = name.getBytes();
		DatagramPacket request = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(LOOKUP_ADDRESS), LOOKUP_PORT);
		socket.send(request);
		return socket;
	}

	@SuppressWarnings("unchecked")
	private static <T> T getServiceResponse(DatagramSocket socket, ClassLoader cl) throws IOException {
		byte[] buffer = new byte[4096];
		DatagramPacket response = new DatagramPacket(buffer, buffer.length);
		socket.receive(response);
		return (T) Cerealizer.unpack(response.getData(), cl);
	}

	public static <T> List<T> lookupServices(String name, int timeout) {
		List<T> l = lookupServices(name, timeout, SocketServer.class.getClassLoader());
		return l;
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> lookupServices(String name, int timeout, ClassLoader cl) {
		try {
			long deadline = System.currentTimeMillis() + timeout;
			DatagramSocket socket = sendServiceQuery(name);
			List<T> returnValue = new ArrayList<T>();
			while (deadline > System.currentTimeMillis()) {
				try {
					socket.setSoTimeout((int) (deadline - System.currentTimeMillis()));
					returnValue.add((T) getServiceResponse(socket, cl));
				} catch (SocketTimeoutException e) {
				}
			}
			return returnValue;
		} catch (Exception e) {
			warn(SocketServer.class, "Error trying to lookup " + name, e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T lookupService(String name, int timeout) {
		T rval = (T) lookupService(name, timeout, SocketServer.class.getClassLoader());
		return rval;
	}

	@SuppressWarnings("unchecked")
	public <T> T lookupService(String name, int timeout, ClassLoader cl) {
		try {
			DatagramSocket socket = sendServiceQuery(name);
			socket.setSoTimeout(timeout);
			Object myResponse = lookupResponder.response(address);
			T returnValue = (T) myResponse;
			while (myResponse.equals(returnValue)) {
				try {
					returnValue = (T) getServiceResponse(socket, cl);
				} catch (SocketTimeoutException e) {
					return null;
				}
			}
			return returnValue;
		} catch (Exception e) {
			warn(SocketServer.class, "Error trying to lookup " + name, e);
			return null;
		}
	}

	private Class<T> handlerClass = null;

	private InetSocketAddress address = null;

	private ServerSocketChannel serverSocket = null;

	private Object[] handlerConstructorArguments = null;

	private Constructor<T> handlerConstructor = null;

	private Map<SocketAddress, T> handlerByAddress = new ConcurrentHashMap<SocketAddress, T>();

	private SelectionKey selectionKey = null;

	private boolean allowLocalPortSearch = false;

	private String serviceName = null;

	private MulticastThread multicastThread = null;

	private LookupResponder lookupResponder = new DefaultLookupResponder();

	/**
	 * Sets up this server with the parameters we have.
	 */
	public SocketServer<T> init() {
		try {
			serverSocket = ServerSocketChannel.open();
			serverSocket.configureBlocking(false);
			serverSocket.socket().setReuseAddress(true);
			boolean unbound = true;
			while (unbound) {
				try {
					serverSocket.socket().bind(address);
					address = (InetSocketAddress) serverSocket.socket().getLocalSocketAddress();
					unbound = false;
				} catch (BindException e) {
					if (allowLocalPortSearch) {
						address = null;
					} else {
						throw e;
					}
				} catch (Exception e) {
					new RuntimeException("Error trying to bind socket to " + address, e);
				}
			}
			SelectorThread.getInstance().register(this);
			if (serviceName != null) {
				multicastThread = new MulticastThread(serviceName, lookupResponder);
				multicastThread.start();
			}
			return this;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Set the lookup responder that will reply when someone tries to look us up.
	 * 
	 * @param responder
	 *            the LookupResponder to use
	 */
	public SocketServer<T> setLookupResponder(LookupResponder r) {
		lookupResponder = r;
		return this;
	}

	/**
	 * Set the service name that this server will respond to on the multicast socket.
	 * 
	 * @param s
	 *            the service name
	 */
	public SocketServer<T> setServiceName(String s) {
		serviceName = s;
		return this;
	}

	protected void setSelectionKey(SelectionKey key) {
		selectionKey = key;
	}

	/**
	 * Get the server socket channel of this server.
	 * 
	 * @return the server socket channel
	 */
	public ServerSocketChannel getServerSocket() {
		return serverSocket;
	}

	/**
	 * Decide if the server shall be allowed to search for another free port if the first one is already used.
	 * 
	 * @param allow
	 *            whether to allow local port search
	 * @return this server to call more methods on
	 */
	public SocketServer<T> setAllowLocalPortSearch(boolean allow) {
		allowLocalPortSearch = allow;
		return this;
	}

	@SuppressWarnings("unchecked")
	private Constructor<T>[] getConstructors(Class<T> h) {
		return (Constructor<T>[]) h.getConstructors();
	}

	/**
	 * Defines what class will manage the connections of this server.
	 * 
	 * @param h
	 *            the {@link SocketServerEventHandler} class that will be used to construct handlers for this server
	 * @param constructorArguments
	 *            the arguments to be sent to the java.lang.reflect#Constructor that will construct the handler
	 *            instances
	 * @return this SocketServer, to call more methods on
	 */
	public SocketServer<T> setHandler(Class<T> h, Object... constructorArguments) {
		try {
			Constructor<T>[] constructors = getConstructors(h);
			Class[] parameterTypes;
			handlerConstructor = null;
			for (int i = 0; i < constructors.length; i++) {
				if ((parameterTypes = constructors[i].getParameterTypes()).length == constructorArguments.length) {
					boolean isOk = true;
					for (int j = 0; j < constructorArguments.length; j++) {
						isOk &= parameterTypes[j].isInstance(constructorArguments[j]);
					}
					if (isOk) {
						handlerConstructor = constructors[i];
						handlerConstructorArguments = constructorArguments;
						break;
					}
				}
			}
			if (handlerConstructor == null) {
				throw new NoSuchMethodException("" + h.getName() + ".<init>(" + Arrays.asList(constructorArguments) + ")");
			}
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Defines what address this server will listen on.
	 * 
	 * @param a
	 *            the address to listen on
	 * @return this SocketServer, to call more methods on
	 */
	public SocketServer<T> setAddress(InetSocketAddress a) {
		address = a;
		return this;
	}

	/**
	 * Create the appropriate network event handler for this server, and registers it for OP_READ.
	 * 
	 * @param selectionKey
	 *            the key that caused the OP_ACCEPT to happen
	 */
	protected void handleAccept(SelectionKey selectionKey) throws Exception {
		SocketChannel connection = serverSocket.accept();
		connection.configureBlocking(false);
		SelectionKey connectionKey = connection.register(selectionKey.selector(), SelectionKey.OP_READ);
		SocketServerEventHandler handler = handlerConstructor.newInstance(handlerConstructorArguments).setServer(this).setSelectionKey(connectionKey);
		connectionKey.attach(handler);
		handler.handleConnect();
	}

	/**
	 * Stops this server from further actions and closes all its connections.
	 */
	public void close() {
		try {
			if (multicastThread != null) {
				multicastThread.kill();
			}
			selectionKey.cancel();
			getServerSocket().close();
			for (Map.Entry<SocketAddress, T> entry : handlerByAddress.entrySet()) {
				entry.getValue().close();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get the connections already made with this server.
	 * 
	 * @return a map with the connections this server has made and received so far
	 */
	protected Map<SocketAddress, T> getHandlerByAddress() {
		return handlerByAddress;
	}

	/**
	 * Make a new (or reuse an old) connection to a given address, and return a new handler for that connection.
	 * 
	 * @param addres
	 *            where to connect
	 * @return a new handler wrapping that connection
	 */
	@SuppressWarnings("unchecked")
	public T getHandler(SocketAddress address) throws ConnectException {
		try {
			T returnValue;
			if ((returnValue = (T) handlerByAddress.get(address)) != null) {
				return returnValue;
			} else {
				return (T) handlerConstructor.newInstance(handlerConstructorArguments).setServer(this).connect(address);
			}
		} catch (ConnectException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}