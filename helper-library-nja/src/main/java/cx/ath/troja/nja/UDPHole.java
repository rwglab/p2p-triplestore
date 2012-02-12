/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.warn;

import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.MessageFormat;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDPHole extends OutputStream {

	public static final String pingMessageFormat = "cx.ath.troja.nja.UDPHole.pingMessage({0}, {1})";

	public static final Pattern pingMessagePattern = Pattern.compile("cx.ath.troja.nja.UDPHole.pingMessage\\((.*), (.*)\\)");

	public static final String pongMessageFormat = "cx.ath.troja.nja.UDPHole.pongMessage({0}, {1})";

	public static final Pattern pongMessagePattern = Pattern.compile("cx.ath.troja.nja.UDPHole.pongMessage\\((.*), (.*)\\)");

	public static final String introspectionMessage = "cx.ath.troja.nja.UDPHole.introspectionMessage()";

	public static final String introspectionResponseMessageFormat = "cx.ath.troja.nja.UDPHole.introspectionResponseMessage({0})";

	public static final Pattern introspectionResponseMessagePattern = Pattern
			.compile("cx.ath.troja.nja.UDPHole.introspectionResponseMessage\\((.*)\\)");

	private class UDPHoleHandler extends DatagramEventHandler {
		@Override
		public void handleRead(byte[] bytes, SocketAddress source) {
			try {
				String message = new String(bytes);
				Matcher matcher = null;
				if ((matcher = pingMessagePattern.matcher(message)).matches() && matcher.group(2).equals(UDPHole.this.identity)
						&& matcher.group(1).equals(UDPHole.this.remoteIdentity)) {

					UDPHole.this.confirmRemoteAddress((InetSocketAddress) source);
					String pongMessage = MessageFormat.format(pongMessageFormat, UDPHole.this.identity, UDPHole.this.remoteIdentity);
					write(new DatagramPacket(pongMessage.getBytes(), pongMessage.getBytes().length, UDPHole.this.confirmedRemoteAddress));
				} else if ((matcher = pongMessagePattern.matcher(message)).matches() && matcher.group(2).equals(UDPHole.this.identity)
						&& matcher.group(1).equals(UDPHole.this.remoteIdentity)) {

					UDPHole.this.confirmRemoteAddress((InetSocketAddress) source);
				} else if ((matcher = introspectionResponseMessagePattern.matcher(message)).matches() && source.equals(UDPHole.this.registry)) {

					String[] hostAndPort = matcher.group(1).split(":");
					UDPHole.this.setPublicAddress(new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
				} else {
					UDPHole.this.outputStream.write(bytes);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public interface AddressHandler {
		public void done(InetSocketAddress address);
	}

	private String identity = null;

	private String remoteIdentity = null;

	private InetSocketAddress confirmedRemoteAddress = null;

	private ScheduledFuture keepaliveTask = null;

	private OutputStream outputStream = null;

	private Datagrammer datagrammer = null;

	private UDPHoleHandler handler = new UDPHoleHandler();;

	private boolean privateExecutor = false;

	private FriendlyScheduledThreadPoolExecutor executor;

	private InetSocketAddress publicAddress = null;

	private ScheduledFuture introspectTask = null;

	private InetSocketAddress registry = null;

	private FutureTask introspectionFuture = new FutureTask<Object>(new Runnable() {
		public void run() {
		}
	}, null);

	private AddressHandler addressHandler = null;

	public UDPHole() {
	}

	public UDPHole setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
		return this;
	}

	public UDPHole setExecutor(FriendlyScheduledThreadPoolExecutor executor) {
		this.executor = executor;
		return this;
	}

	public UDPHole setDatagrammer(Datagrammer datagrammer) {
		this.datagrammer = datagrammer;
		return this;
	}

	public UDPHole setIdentity(String identity) {
		this.identity = identity;
		return this;
	}

	public UDPHole setRemoteIdentity(String remoteIdentity) {
		this.remoteIdentity = remoteIdentity;
		return this;
	}

	@Override
	public void flush() {
	}

	private void introspectDefaults() {
		if (datagrammer == null) {
			datagrammer = new Datagrammer().setHandler(handler).init();
		}
		if (executor == null) {
			executor = new FriendlyScheduledThreadPoolExecutor(1);
			privateExecutor = true;
		}
	}

	private void connectDefaults() {
		introspectDefaults();
		if (outputStream == null) {
			outputStream = System.out;
		}
		if (identity == null) {
			identity = UDPHole.class.getName();
		}
		if (remoteIdentity == null) {
			remoteIdentity = UDPHole.class.getName();
		}
	}

	public void connect(InetSocketAddress remoteAddress) {
		connectDefaults();

		try {
			String message = MessageFormat.format(UDPHole.pingMessageFormat, identity, remoteIdentity);
			for (int i = 0; i < 50; i++) {
				handler.write(new DatagramPacket(message.getBytes(), message.getBytes().length, new InetSocketAddress(remoteAddress.getHostName(),
						remoteAddress.getPort() + i)));
				if (i > 0) {
					handler.write(new DatagramPacket(message.getBytes(), message.getBytes().length, new InetSocketAddress(
							remoteAddress.getHostName(), remoteAddress.getPort() - i)));
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String getIdentity() {
		return identity;
	}

	public String getRemoteIdentity() {
		return remoteIdentity;
	}

	public boolean connected() {
		return confirmedRemoteAddress != null;
	}

	@Override
	public void write(byte[] data, int offset, int length) {
		if (!connected()) {
			throw new RuntimeException("" + this + " is not yet connected.");
		}
		try {
			handler.write(new DatagramPacket(data, offset, length, confirmedRemoteAddress));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void write(byte[] data) {
		write(data, 0, data.length);
	}

	@Override
	public void write(int b) {
		write(new byte[] { (byte) b });
	}

	@Override
	public void close() {
		if (datagrammer != null) {
			datagrammer.close();
		}
		if (privateExecutor) {
			executor.shutdownNow();
		}
	}

	private void confirmRemoteAddress(InetSocketAddress remoteAddress) {
		confirmedRemoteAddress = remoteAddress;
		if (keepaliveTask == null || keepaliveTask.isCancelled()) {
			keepaliveTask = executor.scheduleAtFixedRate(new Runnable() {
				public void run() {
					String message = MessageFormat.format(UDPHole.pingMessageFormat, UDPHole.this.identity, UDPHole.this.remoteIdentity);
					try {
						UDPHole.this.handler.write(new DatagramPacket(message.getBytes(), message.getBytes().length, confirmedRemoteAddress));
					} catch (Exception e) {
						warn(this, "While trying to send keepalive message '" + message + "' to " + UDPHole.this.confirmedRemoteAddress, e);
						throw new RuntimeException(e);
					}
				}
			}, 5, 5, TimeUnit.SECONDS);
		}
	}

	public InetSocketAddress getPublicAddress() {
		return publicAddress;
	}

	private void setPublicAddress(InetSocketAddress address) {
		publicAddress = address;
		introspectionFuture.run();
		if (addressHandler != null) {
			addressHandler.done(address);
			addressHandler = null;
		}
	}

	public InetSocketAddress introspect(InetSocketAddress registry) {
		try {
			introspect(registry, null);
			introspectionFuture.get();
			return getPublicAddress();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void introspect(InetSocketAddress registry, AddressHandler doneHandler) {
		try {
			introspectDefaults();
			this.registry = registry;
			introspectionFuture = new FutureTask<Object>(new Runnable() {
				public void run() {
				}
			}, null);
			if (introspectTask != null && !introspectTask.isCancelled()) {
				introspectTask.cancel(true);
			}
			this.addressHandler = doneHandler;
			introspectTask = executor.scheduleAtFixedRate(new Runnable() {
				public void run() {
					try {
						UDPHole.this.handler.write(new DatagramPacket(introspectionMessage.getBytes(), introspectionMessage.getBytes().length,
								UDPHole.this.registry));
					} catch (Exception e) {
						warn(this, "While trying to introspect at " + UDPHole.this.registry, e);
						throw new RuntimeException(e);
					}
				}
			}, 0, 5, TimeUnit.SECONDS);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
