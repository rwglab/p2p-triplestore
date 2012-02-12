/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.gui;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RadialGradientPaint;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.JPanel;

import cx.ath.troja.chordless.Chord;
import cx.ath.troja.chordless.commands.Command;
import cx.ath.troja.nja.Cerealizer;
import cx.ath.troja.nja.Checksum;
import cx.ath.troja.nja.Identifier;

public class MonitorPanel extends JPanel {

	public static final Color NEON = new Color(0x7f, 0xff, 0x00);

	private class Blip {
		private Color color;

		private double size;

		private double originalSize;

		private Double angle;

		private double stretch;

		private String label;

		private Chord.LoadMonitorMessage load;

		private Chord.RoutingMonitorMessage route;

		public Blip(Color c, double s, Double a, double st) {
			color = c;
			setSize(s);
			angle = a;
			stretch = st;
			label = null;
			load = null;
			route = null;
		}

		public Blip(Color c, double s, Identifier i, double st) {
			this(c, s, MonitorPanel.this.getAngle(i), st);
		}

		public void setSize(double s) {
			size = s;
			originalSize = s;
		}

		public int hashCode() {
			return MonitorPanel.this.keyFor(angle).hashCode() + color.hashCode();
		}

		public Double getAngle() {
			return angle;
		}

		public Color getColor() {
			return color;
		}

		public Chord.LoadMonitorMessage getLoad() {
			return load;
		}

		public Chord.RoutingMonitorMessage getRoute() {
			return route;
		}

		public void refresh(Blip blip) {
			setSize(5 * Math.log(size) + blip.getSize());
			setLoad(blip.getLoad());
			setRoute(blip.getRoute());
		}

		public Blip setRoute(Chord.RoutingMonitorMessage m) {
			route = m;
			return this;
		}

		public double getSize() {
			return size;
		}

		public boolean equals(Object o) {
			if (o instanceof Blip) {
				Blip other = (Blip) o;
				return MonitorPanel.this.keyFor(angle).equals(MonitorPanel.this.keyFor(other.getAngle())) && color.equals(other.getColor());
			} else {
				return false;
			}
		}

		public Blip setLabel(String l) {
			label = l;
			return this;
		}

		public Blip setLoad(Chord.LoadMonitorMessage m) {
			load = m;
			return this;
		}

		private Point getLocation() {
			return MonitorPanel.this.getPoint(angle);
		}

		private RadialGradientPaint getPaint() {
			int s = (int) size;
			if (s < 1) {
				s = 1;
			}
			return new RadialGradientPaint(getLocation(), s, new float[] { 0.0F, 1.0F }, new Color[] { color, Color.BLACK });
		}

		public boolean tick() {
			size = size - stretch;
			return size > 1;
		}

		public void paint(Graphics2D g) {
			g.setPaint(getPaint());
			Point location = getLocation();
			g.fill(new Ellipse2D.Float(location.x - (int) size / 2, location.y - (int) size / 2, (int) size, (int) size));
			if (label != null) {
				g.setColor(color);
				g.drawString(label, (int) (location.x + NODE_SIZE), location.y);
			}
			if (load != null) {
				g.setColor(color);
				g.drawString(load.toString(), (int) (location.x + NODE_SIZE), (int) (location.y + NODE_SIZE));
			}
			if (route != null) {
				g.setColor(color);
				g.draw(new Line2D.Double(location, MonitorPanel.this.getPoint(MonitorPanel.this.getAngle(route.predecessor))));
				Identifier last = null;
				for (Identifier i : route.fingers) {
					if (i != null) {
						if (last == null || last != i) {
							g.draw(new Line2D.Double(location, MonitorPanel.this.getPoint(MonitorPanel.this.getAngle(i))));
							last = i;
						}
					}
				}
			}
		}
	}

	private class Line {

		protected final static double FADE = 0.5;

		protected final static double THINNER = 0.8;

		protected Color color;

		protected Double source;

		protected Double destination;

		protected double fader;

		protected double thickness;

		public Line(Color c, Double s, Double d) {
			thickness = 1;
			fader = 0;
			color = c;
			source = s;
			destination = d;
		}

		public Line(Color c, Identifier s, Identifier d) {
			this(c, MonitorPanel.this.getAngle(s), MonitorPanel.this.getAngle(d));
		}

		public int hashCode() {
			return MonitorPanel.this.keyFor(source).hashCode() + MonitorPanel.this.keyFor(destination).hashCode();
		}

		public boolean equals(Object o) {
			if (o instanceof Line) {
				Line other = (Line) o;
				return (MonitorPanel.this.keyFor(source).equals(MonitorPanel.this.keyFor(other.getSource())) && MonitorPanel.this.keyFor(destination)
						.equals(MonitorPanel.this.keyFor(other.getDestination())));
			} else {
				return false;
			}
		}

		public double getThickness() {
			return thickness;
		}

		public void refresh(Line line) {
			color = line.getColor();
			thickness = 5 * Math.log(thickness * 2) + line.getThickness();
		}

		public Color getColor() {
			return color;
		}

		public Double getSource() {
			return source;
		}

		public Double getDestination() {
			return destination;
		}

		public boolean tick() {
			if (thickness > 1) {
				thickness = thickness * THINNER;
			}
			fader += FADE;
			if (fader > 1) {
				color = color.darker();
				fader--;
			}
			return !color.equals(Color.BLACK);
		}

		protected void line(Graphics2D g, Point a, Point b) {
			double length = a.distance(b);
			Point2D.Double normal = new Point2D.Double((b.y - a.y) / length, -(b.x - a.x) / length);
			Color c = color;
			for (double i = 0; i < thickness; i += 0.5) {
				g.setColor(c);
				g.draw(new Line2D.Double(a.x + (normal.x * i), a.y + (normal.y * i), b.x + (normal.x * i), b.y + (normal.y * i)));
				if (((int) i) % ((int) ((thickness / 3) + 1)) == 0) {
					c = c.darker();
				}
			}
			c = color.darker();
			for (double i = 0.5; i < thickness; i += 0.5) {
				g.setColor(c);
				g.draw(new Line2D.Double(a.x - (normal.x * i), a.y - (normal.y * i), b.x - (normal.x * i), b.y - (normal.y * i)));
				if (((int) i) % ((int) ((thickness / 3) + 1)) == 0) {
					c = c.darker();
				}
			}
			g.setColor(color);
			Point2D.Double unit = new Point2D.Double((b.x - a.x) / length, (b.y - a.y) / length);
			Point2D.Double last = new Point2D.Double(a.x, a.y);
			for (double i = 20.0; i < length; i = i + 20.0) {
				Point2D.Double current = new Point2D.Double(a.x + unit.x * i, a.y + unit.y * i);
				g.draw(new Line2D.Double(last.x + normal.x * thickness, last.y + normal.x * thickness, current.x, current.y));
				g.draw(new Line2D.Double(last.x - normal.x * thickness, last.y - normal.x * thickness, current.x, current.y));
				last = current;
			}
		}

		public void paint(Graphics2D g) {
			Point s = MonitorPanel.this.getPoint(source);
			Point d = MonitorPanel.this.getPoint(destination);
			line(g, s, d);
		}
	}

	public final static double MARGIN = 0.1;

	public final static int REFRESH = 20; // times per second

	public final static double NODE_LIFETIME = 0.5; // seconds blips live

	public final static double SERVER_LIFETIME = 1.0; // seconds server blips live

	public final static double LINE_LIFETIME = 0.5; // seconds lines live

	public final static double NODE_SIZE = 20.0; // size of blips

	public final static int STARTUP = 36;

	public final static int TESTRATE = 50;

	public final static int MAX_LEGEND = 10;

	public final static Object NULL_KEY = new Object();

	public final static int N_ANGLES = 512;

	private Map<Blip, Blip> blips;

	private Map<Line, Line> lines;

	private java.util.Timer timer;

	private Map<String, Long> colors;

	private java.util.List<String> sortedColors;

	private Thread listenerThread;

	private MulticastSocket socket;

	private Map<Identifier, Chord.LoadMonitorMessage> loads;

	private Map<Identifier, Chord.RoutingMonitorMessage> routes;

	private TimerTask startTask;

	public Object keyFor(Double d) {
		if (d == null) {
			return NULL_KEY;
		} else {
			return new Integer((int) ((N_ANGLES * 2 * Math.PI) / d.doubleValue()));
		}
	}

	public MonitorPanel() {
		try {
			loads = new HashMap<Identifier, Chord.LoadMonitorMessage>();
			routes = new HashMap<Identifier, Chord.RoutingMonitorMessage>();
			blips = new ConcurrentHashMap<Blip, Blip>();
			lines = new ConcurrentHashMap<Line, Line>();
			colors = new ConcurrentHashMap<String, Long>();
			sortedColors = new ArrayList<String>();
			listenerThread = null;
			socket = null;
			timer = new java.util.Timer();
			timer.schedule(new TimerTask() {
				public void run() {
					MonitorPanel.this.updateBlips();
					MonitorPanel.this.updateLines();
					MonitorPanel.this.repaint();
				}
			}, 0, (int) (1000.0 / REFRESH));
			timer.scheduleAtFixedRate(new TimerTask() {
				private int a = 0;

				public void run() {
					Blip newBlip = new Blip(NEON, NODE_SIZE, new Double(((a * 10) / 360.0) * 2 * Math.PI), NODE_SIZE
							* ((1.0 / NODE_LIFETIME) / REFRESH));
					Line newLine = new Line(NEON, new Double(0), new Double(((a * 10) / 360.0) * 2 * Math.PI));
					MonitorPanel.this.addBlip(newBlip);
					MonitorPanel.this.addLine(newLine);
					a += 1;
					if (a >= STARTUP) {
						cancel();
					}
				}
			}, 0, REFRESH);
			startTask = new TimerTask() {
				public void run() {
					MonitorPanel.this.startListening();
				}
			};
			timer.schedule(startTask, (int) (((1.0 / REFRESH) * 1000)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean listening() {
		return listenerThread != null;
	}

	public void stopListening() {
		if (startTask != null) {
			startTask.cancel();
			startTask = null;
		}
		if (socket != null) {
			socket.close();
		}
		listenerThread = null;
	}

	public void send(boolean what) {
		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] data = Cerealizer.pack(new Boolean(what));
			socket.send(new DatagramPacket(data, data.length, InetAddress.getByName(Chord.MONITOR_SWITCH_ADDRESS), Chord.MONITOR_SWITCH_PORT));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void startListening() {
		if (listenerThread == null) {
			listenerThread = new Thread() {
				public void run() {
					try {
						socket = new MulticastSocket(Chord.MONITOR_PORT);
						socket.joinGroup(InetAddress.getByName(Chord.MONITOR_ADDRESS));
						socket.setTimeToLive(32);
						while (MonitorPanel.this.listening()) {
							byte[] buffer = new byte[4096];
							DatagramPacket input = new DatagramPacket(buffer, buffer.length);
							socket.receive(input);
							Object obj = null;
							try {
								obj = Cerealizer.unpack(input.getData());
							} catch (Exception e) {
								System.err.println("Unable to deserialize message!");
							}
							if (obj instanceof Command.MonitorMessage) {
								MonitorPanel.this.handleMessage((Command.MonitorMessage) obj);
							} else if (obj instanceof Chord.LoadMonitorMessage) {
								MonitorPanel.this.handleMessage((Chord.LoadMonitorMessage) obj);
							} else if (obj instanceof Chord.RoutingMonitorMessage) {
								MonitorPanel.this.handleMessage((Chord.RoutingMonitorMessage) obj);
							} else if (obj != null) {
								System.err.println("" + obj + " is not of a known message type!");
							}
						}
					} catch (SocketException e) {
						if (!e.getMessage().equals("Socket closed")) {
							throw new RuntimeException(e);
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			};
			listenerThread.start();
		}
	}

	public void test() {
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				handleMessage(new Command.MonitorMessage(new Identifier(Checksum.hex(("" + Math.random()).getBytes())), new Identifier(Checksum
						.hex(("" + Math.random()).getBytes())), "" + Math.random(), null));
			}
		}, REFRESH * STARTUP, TESTRATE);
	}

	protected Color colorFor(String s) {
		return new Color(Checksum.sum(s.getBytes()).intValue());
	}

	private void addOccurence(String name) {
		Long occurences = colors.get(name);
		if (occurences == null) {
			occurences = new Long(0);
		}
		colors.put(name, occurences + 1);
		java.util.List<String> newSortedColors = new ArrayList<String>(colors.keySet());
		Collections.sort(newSortedColors, new Comparator<String>() {
			public int compare(String a, String b) {
				return colors.get(b).compareTo(colors.get(a));
			}
		});
		while (newSortedColors.size() > MAX_LEGEND) {
			newSortedColors.remove(newSortedColors.size() - 1);
		}
		Collections.sort(newSortedColors);
		sortedColors = newSortedColors;
	}

	protected void handleMessage(Chord.LoadMonitorMessage message) {
		loads.put(message.source, message);
	}

	protected void handleMessage(Chord.RoutingMonitorMessage message) {
		routes.put(message.source, message);
	}

	protected void handleMessage(Command.MonitorMessage message) {
		addOccurence(message.className);
		addBlip(new Blip(Color.WHITE, NODE_SIZE, message.source, NODE_SIZE * ((1.0 / SERVER_LIFETIME) / REFRESH)).setLabel("" + message.source)
				.setLoad(loads.get(message.source)).setRoute(routes.get(message.source)));
		addBlip(new Blip(Color.WHITE, NODE_SIZE, message.destination, NODE_SIZE * ((1.0 / SERVER_LIFETIME) / REFRESH))
				.setLabel("" + message.destination).setLoad(loads.get(message.destination)).setRoute(routes.get(message.destination)));
		Color c = colorFor(message.className);
		if (message.regarding != null) {
			for (Identifier i : message.regarding) {
				addBlip(new Blip(c, NODE_SIZE, i, NODE_SIZE * ((1.0 / NODE_LIFETIME) / REFRESH)));
				addLine(new Line(c, i, message.destination));
			}
		}
		addLine(new Line(c, message.source, message.destination));
	}

	protected void addLine(Line line) {
		Line existing = lines.get(line);
		if (existing == null) {
			lines.put(line, line);
		} else {
			existing.refresh(line);
		}
	}

	protected void addBlip(Blip blip) {
		Blip existing = blips.get(blip);
		if (existing == null) {
			blips.put(blip, blip);
		} else {
			existing.refresh(blip);
		}
	}

	protected Double getAngle(Identifier i) {
		if (i == null) {
			return null;
		} else {
			return new BigDecimal(i.getValue()).divide(new BigDecimal(Identifier.getMAX_IDENTIFIER().getValue()), 5, RoundingMode.HALF_DOWN)
					.doubleValue() * 2 * Math.PI;
		}
	}

	protected Point getPoint(Double angle) {
		if (angle == null) {
			return new Point((int) (getWidth() / 2), (int) (getHeight() / 2));
		} else {
			double a = (getWidth() - (2 * MARGIN * getWidth())) / 2;
			double b = (getHeight() - (2 * MARGIN * getHeight())) / 2;
			return new Point((int) (getWidth() / 2 + a * Math.cos(angle)), (int) (getHeight() / 2 + b * Math.sin(angle)));
		}
	}

	protected void updateBlips() {
		Iterator<Blip> iterator = blips.keySet().iterator();
		while (iterator.hasNext()) {
			if (!iterator.next().tick()) {
				iterator.remove();
			}
		}
	}

	protected void updateLines() {
		Iterator<Line> iterator = lines.keySet().iterator();
		while (iterator.hasNext()) {
			if (!iterator.next().tick()) {
				iterator.remove();
			}
		}
	}

	private void drawCircle(Graphics2D g) {
		Color c = NEON;
		int left = (int) (getWidth() * MARGIN);
		int top = (int) (getHeight() * MARGIN);
		int width = (int) (getWidth() - (2 * MARGIN * getWidth()));
		int height = (int) (getHeight() - (2 * MARGIN * getHeight()));
		for (int i = 0; i < 3; i++) {
			g.setColor(c);
			g.drawOval(left + i, top + i, width - i * 2, height - i * 2);
			c = c.darker();
		}
		c = NEON.darker();
		for (int i = 1; i < 3; i++) {
			g.setColor(c);
			g.drawOval(left - i, top - i, width + i * 2, height + i * 2);
			c = c.darker();
		}
	}

	private void drawBackground(Graphics2D g) {
		g.setColor(Color.BLACK);
		g.fillRect(0, 0, getWidth(), getHeight());
	}

	private void drawBlips(Graphics2D g) {
		for (Blip blip : blips.keySet()) {
			blip.paint(g);
		}
	}

	private void drawLines(Graphics2D g) {
		for (Line line : lines.keySet()) {
			line.paint(g);
		}
	}

	private void drawLegend(Graphics2D g) {
		int y = 20;
		for (String color : sortedColors) {
			g.setColor(colorFor(color));
			g.drawString(color + " " + colors.get(color), 10, y);
			y += g.getFont().getSize();
		}
	}

	protected void paintComponent(Graphics g) {
		Graphics2D g2d = (Graphics2D) g;
		drawBackground(g2d);
		drawLegend(g2d);
		drawLines(g2d);
		drawCircle(g2d);
		drawBlips(g2d);
	}

}