/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.error;
import static cx.ath.troja.nja.Log.loggable;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.chordless.commands.SendToSuccessorCommand;
import cx.ath.troja.chordless.commands.Sender;
import cx.ath.troja.chordless.commands.SetFingerCommand;
import cx.ath.troja.chordless.commands.SetSuccessorCommand;
import cx.ath.troja.chordless.commands.StabilizeCommand;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Describable;
import cx.ath.troja.nja.FriendlyScheduledThreadPoolExecutor;
import cx.ath.troja.nja.FutureValue;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.Log;
import cx.ath.troja.nja.SelectorThread;
import cx.ath.troja.nja.SocketServer;
import cx.ath.troja.nja.ThreadState;

public class Chord {

	private class ChordCommandSender implements Sender {
		public void send(Object c) {
			if (c instanceof Command) {
				((Command) c).run(Chord.this, this);
			}
		}
	}

	public static class DeadBootstrapException extends RuntimeException {
		public DeadBootstrapException(Throwable t) {
			super(t);
		}
	}

	public static class RoutingMonitorMessage implements Serializable {
		public Identifier source;

		public Identifier predecessor;

		public Set<Identifier> fingers;

		public RoutingMonitorMessage(Identifier s, Identifier p, ServerInfo[] f) {
			source = s;
			predecessor = p;
			fingers = new HashSet<Identifier>();
			for (int i = 0; i < f.length; i++) {
				if (f[i] != null) {
					fingers.add(f[i].getIdentifier());
				}
			}
		}
	}

	public static class LoadMonitorMessage implements Serializable {
		public Identifier source;

		public double selectorLoad;

		public double executorLoad;

		public long queueSize;

		public LoadMonitorMessage(Identifier s, double se, double e, long q) {
			source = s;
			selectorLoad = se;
			executorLoad = e;
			queueSize = q;
		}

		public String toString() {
			return "net: " + (((int) (selectorLoad * 100)) / 100.0) + " task: " + (((int) (executorLoad * 100)) / 100.0) + " q: " + queueSize;
		}
	}

	public static final String EXECUTOR_SERVICE = "executorService";

	public static final String MONITOR_ADDRESS = "224.0.75.72";

	public static final String MONITOR_SWITCH_ADDRESS = "224.0.75.73";

	private static InetAddress monitorAddress;

	public static final int MONITOR_PORT = 7572;

	public static final int MONITOR_SWITCH_PORT = 7573;

	private static DatagramSocket monitorSocket;

	private static boolean doMonitor = false;

	static {
		try {
			monitorSocket = new DatagramSocket();
			monitorAddress = InetAddress.getByName(MONITOR_ADDRESS);
			Thread t = new Thread("Chord.monitorThread") {
				public void run() {
					while (true) {
						try {
							MulticastSocket socket = new MulticastSocket(MONITOR_SWITCH_PORT);
							socket.joinGroup(InetAddress.getByName(MONITOR_SWITCH_ADDRESS));
							socket.setTimeToLive(32);
							while (true) {
								byte[] buffer = new byte[1024];
								DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
								socket.receive(packet);
								doMonitor = ((Boolean) Cerealizer.unpack(packet.getData())).booleanValue();
							}
						} catch (Throwable t) {
							error(Command.class, "Error while reading from monitor switch socket", t);
						}
					}
				}
			};
			t.setDaemon(true);
			t.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static boolean doMonitor() {
		return doMonitor;
	}

	public static void sendMonitor(Object o) {
		try {
			byte[] packet = Cerealizer.pack(o);
			monitorSocket.send(new DatagramPacket(packet, packet.length, monitorAddress, MONITOR_PORT));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Sender chordCommandSender = new ChordCommandSender();

	private SocketServer<CommandChannel> server = null;

	private InetSocketAddress localAddress = new InetSocketAddress("0.0.0.0", 4545);

	private SocketAddress bootstrapAddress = null;

	private Identifier identifier = null;

	private ServerInfo serverInfo = null;

	private ServerInfo predecessor = null;

	// successors are ServerInfo for the immediate successors in the network
	private ServerInfo[] successors = null;

	// fingers[i] is ServerInfo for the successor of (identifier + 2^i)
	private ServerInfo[] fingers = null;

	private long lastPing;

	private int nextStabilizedFinger;

	private long executorTimestamp;

	private long lastExecutorRoundTrip;

	private ScheduledFuture stabilizeTask = null;

	private ScheduledFuture fixFingersTask = null;

	private ScheduledFuture loadMonitorTask = null;

	private ScheduledFuture routingMonitorTask = null;

	private boolean shutdown = true;

	private FutureValue<Object> startingFuture = null;

	private Script script = null;

	private int haste = 1;

	private FriendlyScheduledThreadPoolExecutor executorService = null;

	private long lastTopologyChange;

	private String serviceName = null;

	public static final int N_SUCCESSORS = 16;

	public static final int PREDECESSOR_TIMEOUT_FACTOR = 4;

	public Script getScript() {
		return script;
	}

	public Chord setServiceName(String s) {
		serviceName = s;
		return this;
	}

	public Chord setLocal(InetSocketAddress a) {
		localAddress = a;
		return this;
	}

	public int getHaste() {
		return haste;
	}

	public Chord setHaste(int i) {
		haste = i;
		return this;
	}

	public Chord setBootstrap(SocketAddress b) {
		bootstrapAddress = b;
		return this;
	}

	public Chord setIdentifier(Identifier i) {
		identifier = i;
		return this;
	}

	protected Status newStatus() {
		return new Status();
	}

	public Status getStatus() {
		Status returnValue = newStatus();
		returnValue.klass = getClass();
		returnValue.serverInfo = getServerInfo();
		returnValue.predecessor = getPredecessor();
		returnValue.fingers = getFingerArray();
		returnValue.successors = getSuccessorArray();
		returnValue.executorTimer = getExecutorService().getTimeKeeper().clone();
		returnValue.queueSize = getExecutorService().getTotalQueueSize();
		returnValue.selectorTimer = SelectorThread.getInstance().getTimeKeeper().clone();
		returnValue.localInetAddress = getSocketServer().getServerSocket().socket().getInetAddress();
		returnValue.localPort = getSocketServer().getServerSocket().socket().getLocalPort();
		returnValue.bootstrapAddress = getBootstrapAddress();
		returnValue.logProperties = Log.getProperties();
		returnValue.serviceName = serviceName;
		return returnValue;
	}

	public FriendlyScheduledThreadPoolExecutor getExecutorService() {
		return executorService;
	}

	public SocketAddress getBootstrapAddress() {
		return bootstrapAddress;
	}

	public SocketServer getSocketServer() {
		return server;
	}

	protected Script createScript() {
		return new Script(this);
	}

	public long getLastTopologyChange() {
		return lastTopologyChange;
	}

	public Chord start() {
		startingFuture = new FutureValue<Object>();
		executorService = new FriendlyScheduledThreadPoolExecutor(1, new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread returnValue = new Thread(r);
				returnValue.setName("" + Chord.this.getServerInfo() + " main executor created at " + new Date());
				ThreadState.put(returnValue, ExecutorService.class, EXECUTOR_SERVICE);
				return returnValue;
			}
		});
		lastTopologyChange = System.currentTimeMillis();
		script = createScript();
		server = new SocketServer<CommandChannel>().setHandler(CommandChannel.class, this).setAddress(localAddress).setAllowLocalPortSearch(true)
				.setServiceName(serviceName).init();
		localAddress = (InetSocketAddress) getSocketServer().getServerSocket().socket().getLocalSocketAddress();
		if (identifier == null) {
			identifier = Identifier.generate(localAddress);
		}
		lastPing = 0;
		serverInfo = new ServerInfo(localAddress, identifier);
		fingers = new ServerInfo[identifier.getBITS()];
		nextStabilizedFinger = 0;
		successors = new ServerInfo[N_SUCCESSORS];
		setupNetwork();
		setupTasks();
		shutdown = false;
		startingFuture.set(null);
		return this;
	}

	private void setupNetwork() {
		predecessor = null;
		successors[0] = getServerInfo();
		if (bootstrapAddress != null) {
			join(bootstrapAddress);
		} else if (serviceName != null) {
			bootstrapAddress = server.lookupService(serviceName, 100);
			if (bootstrapAddress != null) {
				join(bootstrapAddress);
			}
		}
	}

	private void join(SocketAddress address) {
		try {
			send(new SendToSuccessorCommand(getServerInfo(), getIdentifier(), new SetSuccessorCommand(getServerInfo())), address);
		} catch (ConnectException e) {
			error(this, "Error while trying to join " + address, e);
			throw new DeadBootstrapException(e);
		}
	}

	protected void stop1() {
		startingFuture = null;
		shutdown = true;
	}

	protected void stop2() {
		if (stabilizeTask != null) {
			stabilizeTask.cancel(true);
		}
		if (fixFingersTask != null) {
			fixFingersTask.cancel(true);
		}
		if (loadMonitorTask != null) {
			loadMonitorTask.cancel(true);
		}
		if (routingMonitorTask != null) {
			routingMonitorTask.cancel(true);
		}
		executorService.shutdownNow();
		executorService = null;
	}

	protected void stop3() {
		server.close();
	}

	public void stop() {
		stop1();
		stop2();
		stop3();
	}

	protected void setupTasks() {
		stabilizeTask = executorService.scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return Chord.class.getName() + ".stabilize";
			}

			public void run() {
				Chord.this.stabilize();
			}
		}, 0, haste, TimeUnit.SECONDS);
		fixFingersTask = executorService.scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return Chord.class.getName() + ".fixFingers";
			}

			public void run() {
				Chord.this.fixFingers();
			}
		}, 0, haste, TimeUnit.SECONDS);
		loadMonitorTask = executorService.scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return Chord.class.getName() + ".monitorLoad";
			}

			public void run() {
				Chord.this.monitorLoad();
			}
		}, 0, haste, TimeUnit.SECONDS);
		routingMonitorTask = executorService.scheduleAtFixedRate(new Describable() {
			public String getDescription() {
				return Chord.class.getName() + ".monitorRouting";
			}

			public void run() {
				Chord.this.monitorRouting();
			}
		}, 0, haste * 10, TimeUnit.SECONDS);
	}

	private void monitorRouting() {
		try {
			if (doMonitor() && getPredecessor() != null) {
				sendMonitor(new RoutingMonitorMessage(getIdentifier(), getPredecessor().getIdentifier(), getFingerArray()));
			}
		} catch (Throwable t) {
			error(this, "Error while running monitorRouting", t);
			throw new RuntimeException(t);
		}
	}

	private void monitorLoad() {
		try {
			if (doMonitor()) {
				sendMonitor(new LoadMonitorMessage(getIdentifier(), SelectorThread.getInstance().getTimeKeeper().load(), getExecutorService()
						.getTimeKeeper().load(), getExecutorService().getTotalQueueSize()));
			}
		} catch (Throwable t) {
			error(this, "Error while running monitorLoad", t);
			throw new RuntimeException(t);
		}
	}

	private void stabilize() {
		try {
			if (loggable(this, DEBUG))
				debug(this, "" + this + " running stabilize()");
			Command com = new StabilizeCommand(getServerInfo());
			try {
				send(com, getSuccessor().getAddress());
			} catch (ConnectException e) {
				debug(this, "Error while trying to send " + com + " to " + successors[0], e);
				shiftSuccessors();
			}
			if (System.currentTimeMillis() - lastPing > (haste * PREDECESSOR_TIMEOUT_FACTOR) * 1000) {
				predecessor = null;
			}
		} catch (Throwable e) {
			error(this, "Error while running stabilize", e);
			throw new RuntimeException(e);
		}
	}

	private void fixFingers() {
		try {
			if (loggable(this, DEBUG))
				debug(this, "" + this + " running fixFingers()");
			sendToSuccessor(getIdentifier().toFingerIdentifier(nextStabilizedFinger), new SetFingerCommand(getServerInfo(), nextStabilizedFinger));
			nextStabilizedFinger = (nextStabilizedFinger + 1) % fingers.length;
		} catch (Throwable e) {
			error(this, "Error while running fixFingers", e);
			throw new RuntimeException(e);
		}
	}

	public void ping(StabilizeCommand c) {
		if (getPredecessor() == null || c.getCaller().getIdentifier().equals(getPredecessor().getIdentifier())) {
			lastPing = System.currentTimeMillis();
		}
	}

	public void shiftSuccessors(int index) {
		for (int i = successors.length - 1; i > index; i--) {
			successors[i - 1] = successors[i];
		}
		successors[successors.length - 1] = null;
		if (successors[0] == null) {
			successors[0] = getServerInfo();
		}
	}

	public void shiftSuccessors() {
		shiftSuccessors(0);
	}

	public void clearFingerAddress(SocketAddress a) {
		for (int i = 0; i < fingers.length; i++) {
			if (fingers[i] != null && fingers[i].getAddress().equals(a)) {
				fingers[i] = null;
			}
		}
	}

	protected Object clone(Object o) {
		return Cerealizer.unpack(Cerealizer.pack(o));
	}

	public void send(Command c, SocketAddress a) throws ConnectException {
		if (localAddress.equals(a)) {
			((Command) clone(c)).run(this, chordCommandSender);
		} else {
			server.getHandler(a).send(c);
		}
	}

	public ServerInfo closestPrecedingFinger(Identifier ident) {
		for (int i = fingers.length - 1; i >= 0; i--) {
			if (fingers[i] != null && fingers[i].getIdentifier().betweenGT_LTE(getIdentifier(), ident)) {
				return fingers[i];
			}
		}
		return successors[0];
	}

	public void sendToSuccessor(Identifier i, Command command) {
		new SendToSuccessorCommand(getServerInfo(), i, command).run(this, chordCommandSender);
	}

	public String toString() {
		return "<" + this.getClass().getName() + " serverInfo='" + getServerInfo() + "'>";
	}

	public void setPredecessor(ServerInfo p) {
		if (predecessor == null || !predecessor.getIdentifier().equals(p.getIdentifier())) {
			lastTopologyChange = System.currentTimeMillis();
		}
		predecessor = p;
	}

	public ServerInfo getPredecessor() {
		return predecessor;
	}

	public Identifier getIdentifier() {
		return identifier;
	}

	public ServerInfo getServerInfo() {
		return serverInfo;
	}

	public ServerInfo[] getSuccessorArray() {
		ServerInfo[] returnValue = new ServerInfo[successors.length];
		System.arraycopy(successors, 0, returnValue, 0, successors.length);
		return returnValue;
	}

	public ServerInfo getSuccessor() {
		return successors[0];
	}

	public void setExtraSuccessors(ServerInfo[] extra) {
		System.arraycopy(extra, 0, successors, 1, successors.length - 1);
	}

	public void setSuccessor(ServerInfo info) {
		if (info == null) {
			throw new RuntimeException("ServerInfo must not be null!");
		}
		if (!successors[0].getIdentifier().equals(info.getIdentifier())) {
			lastTopologyChange = System.currentTimeMillis();
		}
		successors[0] = info;
	}

	public ServerInfo getFinger(int i) {
		return fingers[i];
	}

	public ServerInfo[] getFingerArray() {
		ServerInfo[] returnValue = new ServerInfo[fingers.length];
		System.arraycopy(fingers, 0, returnValue, 0, fingers.length);
		return returnValue;
	}

	public void setFinger(ServerInfo info, int i) {
		fingers[i] = info;
	}

	public FutureValue<Object> getStartingFuture() {
		return startingFuture;
	}

	public boolean isShutdown() {
		return shutdown;
	}

}
