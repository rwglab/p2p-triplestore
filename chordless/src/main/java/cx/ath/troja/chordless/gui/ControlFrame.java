/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */
/*
 * ControlFrame.java Created on January 19, 2009, 3:23 PM
 */

package cx.ath.troja.chordless.gui;

import java.awt.event.KeyEvent;
import java.io.EOFException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JOptionPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import cx.ath.troja.chordless.ServerInfo;
import cx.ath.troja.chordless.commands.LogPropertiesCommand;
import cx.ath.troja.chordless.commands.ScriptExecutionCommand;
import cx.ath.troja.chordless.commands.StatusCommand;
import cx.ath.troja.chordless.commands.TimeKeeperResetCommand;
import cx.ath.troja.chordless.dhash.DHashStatus;
import cx.ath.troja.chordless.dhash.commands.DHashRestartCommand;
import cx.ath.troja.chordless.dhash.commands.DHashStatusCommand;
import cx.ath.troja.chordless.tools.ChordProxy;
import cx.ath.troja.chordless.tools.RemoteChordProxy;
import cx.ath.troja.nja.Identifier;
import cx.ath.troja.nja.RuntimeArguments;
import cx.ath.troja.nja.TimeKeeper;

/**
 * 
 * @author zond
 */
public class ControlFrame extends javax.swing.JFrame {

	private static RuntimeArguments arguments;

	public static void main(String[] args) throws ConnectException {
		arguments = new RuntimeArguments(args);
		if (arguments.get("host") == null || arguments.get("port") == null) {
			System.out.println("Usage: ControlFrame host=HOST port=PORT");
		} else {
			new ControlFrame(new RemoteChordProxy(new InetSocketAddress(arguments.get("host"), Integer.parseInt(arguments.get("port")))))
					.setVisible(true);
		}
	}

	private class WaitingCommandsTableModel extends javax.swing.table.AbstractTableModel {
		private ArrayList<ArrayList<String>> rows;

		public WaitingCommandsTableModel() {
			rows = new ArrayList<ArrayList<String>>();
		}

		public void updateStatus() {
			Map<String, Integer> waiting = ControlFrame.this.getStatus().waitingCommands;
			List<ArrayList<String>> newRows = new LinkedList<ArrayList<String>>();
			for (Map.Entry<String, Integer> mapEntry : waiting.entrySet()) {
				ArrayList<String> newRow = new ArrayList<String>(2);
				String[] className = mapEntry.getKey().split("\\.");
				newRow.add(className[className.length - 1]);
				newRow.add("" + mapEntry.getValue());
				newRows.add(newRow);
			}
			rows = new ArrayList<ArrayList<String>>(newRows);
			fireTableDataChanged();
		}

		public int getRowCount() {
			return rows.size();
		}

		public int getColumnCount() {
			return 2;
		}

		public Object getValueAt(int row, int col) {
			return rows.get(row).get(col);
		}

		public String getColumnName(int col) {
			switch (col) {
			case 0:
				return "Class";
			case 1:
				return "Number";
			}
			return "";
		}
	}

	private class ProfileTableModel extends javax.swing.table.AbstractTableModel {
		private ArrayList<ArrayList<String>> rows;

		public ProfileTableModel() {
			rows = new ArrayList<ArrayList<String>>();
		}

		public void updateStatus() {
			Map<String, TimeKeeper.Occurence> occurences = ControlFrame.this.getStatus().executorTimer.getCounts();
			List<ArrayList<String>> newRows = new LinkedList<ArrayList<String>>();
			for (Map.Entry<String, TimeKeeper.Occurence> mapEntry : occurences.entrySet()) {
				ArrayList<String> newRow = new ArrayList<String>(4);
				newRow.add(mapEntry.getKey());
				newRow.add("" + mapEntry.getValue().times());
				newRow.add("" + ((long) (mapEntry.getValue().duration() / 1000000.0)));
				newRow.add("" + ((long) (mapEntry.getValue().averageDuration() / 1000000.0)));
				newRow.add("" + ((long) (mapEntry.getValue().max() / 1000000.0)));
				newRow.add("" + ((long) mapEntry.getValue().averageWait()));
				newRow.add("" + ((long) mapEntry.getValue().maxWait()));
				newRows.add(newRow);
			}
			rows = new ArrayList<ArrayList<String>>(newRows);
			fireTableDataChanged();
		}

		public int getRowCount() {
			return rows.size();
		}

		public int getColumnCount() {
			return 7;
		}

		public Object getValueAt(int row, int col) {
			return rows.get(row).get(col);
		}

		public String getColumnName(int col) {
			switch (col) {
			case 0:
				return "Description";
			case 1:
				return "Times";
			case 2:
				return "Sum (ms)";
			case 3:
				return "Avg (ms)";
			case 4:
				return "Max (ms)";
			case 5:
				return "Avg Wait (ms)";
			case 6:
				return "Max Wait (ms)";
			}
			return "";
		}
	}

	private class SuccessorListModel extends javax.swing.AbstractListModel {
		private java.util.List<String> entries;

		public SuccessorListModel() {
			entries = new ArrayList<String>();
		}

		public void updateStatus() {
			List<String> newList = new LinkedList<String>();
			ServerInfo[] successors = ControlFrame.this.getStatus().successors;
			for (int i = 0; i < successors.length; i++) {
				if (successors[i] != null) {
					newList.add("" + i + ": " + successors[i].toShortString());
				}
			}
			entries = new ArrayList<String>(newList);
			fireContentsChanged(this, 0, entries.size() - 1);
		}

		public Object getElementAt(int i) {
			return entries.get(i);
		}

		public int getSize() {
			return entries.size();
		}
	}

	private class FingerListModel extends javax.swing.AbstractListModel {
		private java.util.List<String> entries;

		private boolean sameFinger(ServerInfo s1, ServerInfo s2) {
			if (s1 == null && s2 == null) {
				return true;
			} else if (s1 == null) {
				return false;
			} else if (s2 == null) {
				return false;
			} else {
				return s1.equals(s2);
			}
		}

		public FingerListModel() {
			entries = new ArrayList<String>();
		}

		public void updateStatus() {
			List<String> newList = new LinkedList<String>();
			ServerInfo[] fingers = ControlFrame.this.getStatus().fingers;
			int firstPosition = 0;
			int lastPosition = 0;
			for (int i = 1; i < fingers.length; i++) {
				if (sameFinger(fingers[i - 1], fingers[i])) {
					lastPosition = i;
				} else {
					newList.add("" + firstPosition + "-" + lastPosition + ": " + (fingers[i - 1] == null ? "null" : fingers[i - 1].toShortString()));
					firstPosition = i;
					lastPosition = i;
				}
			}
			newList.add("" + firstPosition + "-" + lastPosition + ": "
					+ (fingers[fingers.length - 1] == null ? "null" : fingers[fingers.length - 1].toShortString()));
			entries = new ArrayList<String>(newList);
			fireContentsChanged(this, 0, entries.size() - 1);
		}

		public Object getElementAt(int i) {
			return entries.get(i);
		}

		public int getSize() {
			return entries.size();
		}
	}

	private class ScriptTracker {
		LinkedList<String> history;

		int historyIndex;

		public ScriptTracker() {
			history = new LinkedList<String>();
			historyIndex = -1;
		}

		public void historyBack() {
			if (history.size() > historyIndex + 1) {
				historyIndex++;
				ControlFrame.this.getExecuteField().setText(history.get(historyIndex));
			}
		}

		public void historyForward() {
			if (historyIndex > 0) {
				historyIndex--;
				ControlFrame.this.getExecuteField().setText(history.get(historyIndex));
			} else if (historyIndex == 0) {
				historyIndex--;
				ControlFrame.this.getExecuteField().setText("");
			}
		}

		public void println(Object s) {
			print("" + s + "\n");
		}

		public void print(Object s) {
			ControlFrame.this.getResponseArea().append("" + s);
			ControlFrame.this.getResponseArea().setCaretPosition(ControlFrame.this.getResponseArea().getText().length());
		}

		public void eval(final String s) {
			ControlFrame.this.getExecuteField().setEnabled(false);
			new Thread() {
				public void run() {
					Object response = ControlFrame.this.getProxy().run(new ScriptExecutionCommand(null, s));
					history.add(0, s);
					println(s);
					ControlFrame.this.getExecuteField().setEnabled(true);
					ControlFrame.this.getExecuteField().grabFocus();
					ScriptTracker.this.println("<" + response + ">");
					ControlFrame.this.getExecuteField().setText("");
					historyIndex = -1;
				}
			}.start();
		}
	}

	private ChordProxy proxy;

	private DHashStatus status;

	private java.util.Timer timer;

	private ScriptTracker scriptTracker;

	private ProfileTableModel profileTableModel;

	private SuccessorListModel successorListModel;

	private FingerListModel fingerListModel;

	private WaitingCommandsTableModel waitingCommandsTableModel;

	private boolean refreshParameters = true;

	private boolean monitoring = false;

	/** Creates new form ControlFrame */
	public ControlFrame(ChordProxy p) {
		proxy = p;
		scriptTracker = new ScriptTracker();
		profileTableModel = new ProfileTableModel();
		successorListModel = new SuccessorListModel();
		fingerListModel = new FingerListModel();
		waitingCommandsTableModel = new WaitingCommandsTableModel();

		initComponents();

		Comparator numericComparator = new Comparator() {
			public int compare(Object o1, Object o2) {
				try {
					return new Integer("" + o1).compareTo(new Integer("" + o2));
				} catch (NumberFormatException e) {
					return 0;
				}
			}
		};
		((javax.swing.DefaultRowSorter<?, ?>) profileTable.getRowSorter()).setComparator(1, numericComparator);
		((javax.swing.DefaultRowSorter<?, ?>) profileTable.getRowSorter()).setComparator(2, numericComparator);
		((javax.swing.DefaultRowSorter<?, ?>) profileTable.getRowSorter()).setComparator(3, numericComparator);
		((javax.swing.DefaultRowSorter<?, ?>) profileTable.getRowSorter()).setComparator(4, numericComparator);
		((javax.swing.DefaultRowSorter<?, ?>) profileTable.getRowSorter()).setComparator(5, numericComparator);
		((javax.swing.DefaultRowSorter<?, ?>) profileTable.getRowSorter()).setComparator(6, numericComparator);
		profileTable.getColumnModel().getColumn(0).setPreferredWidth(500);

		((javax.swing.DefaultRowSorter<?, ?>) waitingTable.getRowSorter()).setComparator(1, numericComparator);
		waitingTable.getColumnModel().getColumn(0).setPreferredWidth(500);

		setupTimer();
	}

	public JTextField getExecuteField() {
		return executeField;
	}

	private void setupTimer() {
		setStatus((DHashStatus) getProxy().run(new StatusCommand(null)));
		timer = new java.util.Timer();
		timer.schedule(new TimerTask() {
			public void run() {
				if (tabbedPane.getSelectedComponent() == dataPanel) {
					ControlFrame.this.setStatus((DHashStatus) ControlFrame.this.getProxy().run(new DHashStatusCommand(null).withData()));
				} else {
					ControlFrame.this.setStatus((DHashStatus) ControlFrame.this.getProxy().run(new DHashStatusCommand(null).withoutData()));
				}
			}
		}, 0, 1000);
	}

	protected JTextArea getResponseArea() {
		return responseArea;
	}

	protected java.util.Timer getTimer() {
		return timer;
	}

	protected DHashStatus getStatus() {
		return status;
	}

	protected void setStatus(DHashStatus s) {
		status = s;
		profileTableModel.updateStatus();
		fingerListModel.updateStatus();
		successorListModel.updateStatus();
		waitingCommandsTableModel.updateStatus();
		updateComponents();
	}

	protected ChordProxy getProxy() {
		return proxy;
	}

	private void updateComponents() {
		if (refreshParameters) {
			identifierField.setText(getStatus().serverInfo.getIdentifier().toString());
			setTitle(getStatus().serverInfo.getIdentifier().toString());
			localAddressField.setText(getStatus().localInetAddress.getHostName());
			localPortField.setText("" + getStatus().localPort);
			InetSocketAddress bootstrapAddress = (InetSocketAddress) getStatus().bootstrapAddress;
			bootstrapAddressField.setText((bootstrapAddress == null ? "" : bootstrapAddress.getHostName()));
			bootstrapPortField.setText("" + (bootstrapAddress == null ? "" : bootstrapAddress.getPort()));
			storageField.setText("" + Arrays.asList(getStatus().storageDescription));
			logPropertiesArea.setText(getStatus().logProperties);
			copiesField.setText("" + getStatus().copies);
			serviceNameField.setText("" + getStatus().serviceName);
			refreshParameters = false;
		}
		predecessorField.setText(getStatus().predecessor == null ? "null" : getStatus().predecessor.toShortString());
		networkLoadLabel.setText("" + (((int) (getStatus().selectorTimer.load() * 1000)) / 1000.0));
		taskLoadLabel.setText("" + (((int) (getStatus().executorTimer.load() * 1000)) / 1000.0));
		entriesHeldField.setText("" + getStatus().entriesHeld);
		entriesOwnedField.setText("" + getStatus().entriesOwned);
		oldestEntryField.setText("" + new Date(getStatus().oldestEntryAge));
		youngestEntryField.setText("" + new Date(getStatus().youngestEntryAge));
		queueLabel.setText("" + getStatus().queueSize);
	}

	/**
	 * This method is called from within the constructor to initialize the form. WARNING: Do NOT modify this code. The
	 * content of this method is always regenerated by the Form Editor.
	 */
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		reconnectDialog = new javax.swing.JDialog();
		jLabel4 = new javax.swing.JLabel();
		reconnectAddressField = new javax.swing.JTextField();
		jLabel14 = new javax.swing.JLabel();
		reconnectPortField = new javax.swing.JTextField();
		jButton4 = new javax.swing.JButton();
		tabbedPane = new javax.swing.JTabbedPane();
		jPanel1 = new javax.swing.JPanel();
		jButton1 = new javax.swing.JButton();
		jLabel7 = new javax.swing.JLabel();
		identifierField = new javax.swing.JTextField();
		jLabel1 = new javax.swing.JLabel();
		localAddressField = new javax.swing.JTextField();
		jLabel2 = new javax.swing.JLabel();
		localPortField = new javax.swing.JTextField();
		jLabel3 = new javax.swing.JLabel();
		storageField = new javax.swing.JTextField();
		jLabel5 = new javax.swing.JLabel();
		bootstrapAddressField = new javax.swing.JTextField();
		jLabel6 = new javax.swing.JLabel();
		bootstrapPortField = new javax.swing.JTextField();
		jLabel13 = new javax.swing.JLabel();
		copiesField = new javax.swing.JTextField();
		jLabel18 = new javax.swing.JLabel();
		serviceNameField = new javax.swing.JTextField();
		jPanel2 = new javax.swing.JPanel();
		jLabel8 = new javax.swing.JLabel();
		predecessorField = new javax.swing.JTextField();
		jLabel9 = new javax.swing.JLabel();
		jLabel10 = new javax.swing.JLabel();
		jScrollPane2 = new javax.swing.JScrollPane();
		successorList = new javax.swing.JList();
		jScrollPane1 = new javax.swing.JScrollPane();
		fingerList = new javax.swing.JList();
		jPanel3 = new javax.swing.JPanel();
		jScrollPane3 = new javax.swing.JScrollPane();
		logPropertiesArea = new javax.swing.JTextArea();
		jButton2 = new javax.swing.JButton();
		jPanel4 = new javax.swing.JPanel();
		jScrollPane4 = new javax.swing.JScrollPane();
		profileTable = new javax.swing.JTable();
		jButton3 = new javax.swing.JButton();
		jLabel11 = new javax.swing.JLabel();
		networkLoadLabel = new javax.swing.JLabel();
		jLabel12 = new javax.swing.JLabel();
		taskLoadLabel = new javax.swing.JLabel();
		jLabel16 = new javax.swing.JLabel();
		queueLabel = new javax.swing.JLabel();
		jPanel5 = new javax.swing.JPanel();
		executeField = new javax.swing.JTextField();
		jScrollPane5 = new javax.swing.JScrollPane();
		responseArea = new javax.swing.JTextArea();
		dataPanel = new javax.swing.JPanel();
		jLabel15 = new javax.swing.JLabel();
		entriesHeldField = new javax.swing.JLabel();
		jLabel17 = new javax.swing.JLabel();
		entriesOwnedField = new javax.swing.JLabel();
		jLabel19 = new javax.swing.JLabel();
		oldestEntryField = new javax.swing.JLabel();
		jLabel21 = new javax.swing.JLabel();
		youngestEntryField = new javax.swing.JLabel();
		jPanel7 = new javax.swing.JPanel();
		jScrollPane6 = new javax.swing.JScrollPane();
		waitingTable = new javax.swing.JTable();
		monitorPanel = new cx.ath.troja.chordless.gui.MonitorPanel();
		monitorSwitch = new javax.swing.JButton();
		jMenuBar1 = new javax.swing.JMenuBar();
		jMenu1 = new javax.swing.JMenu();
		jMenuItem1 = new javax.swing.JMenuItem();
		jMenuItem2 = new javax.swing.JMenuItem();

		jLabel4.setText("Address");

		jLabel14.setText("Port");

		jButton4.setText("Reconnect");
		jButton4.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				reconnect(evt);
			}
		});

		javax.swing.GroupLayout reconnectDialogLayout = new javax.swing.GroupLayout(reconnectDialog.getContentPane());
		reconnectDialog.getContentPane().setLayout(reconnectDialogLayout);
		reconnectDialogLayout.setHorizontalGroup(reconnectDialogLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				reconnectDialogLayout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								reconnectDialogLayout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addGroup(
												reconnectDialogLayout
														.createSequentialGroup()
														.addGroup(
																reconnectDialogLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
																		.addComponent(jLabel4).addComponent(jLabel14))
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
														.addGroup(
																reconnectDialogLayout
																		.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
																		.addComponent(reconnectPortField, javax.swing.GroupLayout.DEFAULT_SIZE, 285,
																				Short.MAX_VALUE)
																		.addComponent(reconnectAddressField, javax.swing.GroupLayout.DEFAULT_SIZE,
																				285, Short.MAX_VALUE)))
										.addComponent(jButton4, javax.swing.GroupLayout.Alignment.TRAILING)).addContainerGap()));
		reconnectDialogLayout.setVerticalGroup(reconnectDialogLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				reconnectDialogLayout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								reconnectDialogLayout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
										.addComponent(jLabel4, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(reconnectAddressField, javax.swing.GroupLayout.Alignment.LEADING))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								reconnectDialogLayout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
										.addComponent(jLabel14, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE,
												Short.MAX_VALUE).addComponent(reconnectPortField))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
						.addComponent(jButton4).addContainerGap()));

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
		setTitle("Chordless");

		tabbedPane.addChangeListener(new javax.swing.event.ChangeListener() {
			public void stateChanged(javax.swing.event.ChangeEvent evt) {
				tabChanged(evt);
			}
		});

		jButton1.setText("Restart");
		jButton1.setAlignmentX(1.0F);
		jButton1.setAlignmentY(1.0F);
		jButton1.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				restartChord(evt);
			}
		});

		jLabel7.setText("Identifier");

		jLabel1.setText("Local address");

		jLabel2.setText("Local port");

		jLabel3.setText("Storage");

		jLabel5.setText("Bootstrap address");

		jLabel6.setText("Bootstrap port");

		jLabel13.setText("Copies");

		jLabel18.setText("Service name");

		serviceNameField.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				serviceNameFieldActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1.setLayout(jPanel1Layout);
		jPanel1Layout.setHorizontalGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel1Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addGroup(
												jPanel1Layout
														.createSequentialGroup()
														.addGroup(
																jPanel1Layout
																		.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
																		.addComponent(jLabel13, javax.swing.GroupLayout.DEFAULT_SIZE, 108,
																				Short.MAX_VALUE)
																		.addComponent(jLabel1, javax.swing.GroupLayout.DEFAULT_SIZE, 108,
																				Short.MAX_VALUE)
																		.addComponent(jLabel2, javax.swing.GroupLayout.DEFAULT_SIZE, 108,
																				Short.MAX_VALUE)
																		.addComponent(jLabel3, javax.swing.GroupLayout.DEFAULT_SIZE, 108,
																				Short.MAX_VALUE)
																		.addComponent(jLabel5, javax.swing.GroupLayout.DEFAULT_SIZE, 108,
																				Short.MAX_VALUE)
																		.addComponent(jLabel6, javax.swing.GroupLayout.DEFAULT_SIZE, 108,
																				Short.MAX_VALUE)
																		.addComponent(jLabel18, javax.swing.GroupLayout.PREFERRED_SIZE, 108,
																				javax.swing.GroupLayout.PREFERRED_SIZE)
																		.addComponent(jLabel7, javax.swing.GroupLayout.Alignment.TRAILING,
																				javax.swing.GroupLayout.PREFERRED_SIZE, 108,
																				javax.swing.GroupLayout.PREFERRED_SIZE))
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
														.addGroup(
																jPanel1Layout
																		.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
																		.addComponent(copiesField, javax.swing.GroupLayout.Alignment.TRAILING,
																				javax.swing.GroupLayout.DEFAULT_SIZE, 624, Short.MAX_VALUE)
																		.addComponent(bootstrapPortField, javax.swing.GroupLayout.Alignment.TRAILING,
																				javax.swing.GroupLayout.DEFAULT_SIZE, 624, Short.MAX_VALUE)
																		.addComponent(bootstrapAddressField,
																				javax.swing.GroupLayout.Alignment.TRAILING,
																				javax.swing.GroupLayout.DEFAULT_SIZE, 624, Short.MAX_VALUE)
																		.addComponent(storageField, javax.swing.GroupLayout.Alignment.TRAILING,
																				javax.swing.GroupLayout.DEFAULT_SIZE, 624, Short.MAX_VALUE)
																		.addComponent(localPortField, javax.swing.GroupLayout.Alignment.TRAILING,
																				javax.swing.GroupLayout.DEFAULT_SIZE, 624, Short.MAX_VALUE)
																		.addComponent(localAddressField, javax.swing.GroupLayout.DEFAULT_SIZE, 624,
																				Short.MAX_VALUE)
																		.addComponent(identifierField, javax.swing.GroupLayout.DEFAULT_SIZE, 624,
																				Short.MAX_VALUE)
																		.addComponent(serviceNameField, javax.swing.GroupLayout.DEFAULT_SIZE, 624,
																				Short.MAX_VALUE)))
										.addComponent(jButton1, javax.swing.GroupLayout.Alignment.TRAILING)).addContainerGap()));

		jPanel1Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] { jLabel1, jLabel13, jLabel18, jLabel2, jLabel3,
				jLabel5, jLabel6 });

		jPanel1Layout.setVerticalGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel1Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel7, javax.swing.GroupLayout.DEFAULT_SIZE, 17, Short.MAX_VALUE)
										.addComponent(identifierField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel1, javax.swing.GroupLayout.DEFAULT_SIZE, 17, Short.MAX_VALUE)
										.addComponent(localAddressField, javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel2, javax.swing.GroupLayout.DEFAULT_SIZE, 17, Short.MAX_VALUE)
										.addComponent(localPortField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel3, javax.swing.GroupLayout.DEFAULT_SIZE, 17, Short.MAX_VALUE)
										.addComponent(storageField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel5)
										.addComponent(bootstrapAddressField, javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel6)
										.addComponent(bootstrapPortField, javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel13, javax.swing.GroupLayout.PREFERRED_SIZE, 14, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addComponent(copiesField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel18, javax.swing.GroupLayout.DEFAULT_SIZE, 17, Short.MAX_VALUE)
										.addComponent(serviceNameField))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 28, Short.MAX_VALUE).addComponent(jButton1)
						.addContainerGap()));

		jPanel1Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] { bootstrapPortField, identifierField, jLabel1, jLabel2,
				jLabel3, jLabel6, jLabel7, localAddressField, localPortField, storageField });

		jPanel1Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] { bootstrapAddressField, jLabel5 });

		jPanel1Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] { copiesField, jLabel13 });

		jPanel1Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] { jLabel18, serviceNameField });

		tabbedPane.addTab("Parameters", jPanel1);

		jLabel8.setText("Predecessor");

		predecessorField.setEditable(false);
		predecessorField.setFont(new java.awt.Font("Monospaced", 0, 11));

		jLabel9.setText("Successors");

		jLabel10.setText("Fingers");

		successorList.setFont(new java.awt.Font("Monospaced", 0, 11));
		successorList.setModel(successorListModel);
		successorList.setEnabled(false);
		jScrollPane2.setViewportView(successorList);

		fingerList.setFont(new java.awt.Font("Monospaced", 0, 11));
		fingerList.setModel(fingerListModel);
		fingerList.setEnabled(false);
		jScrollPane1.setViewportView(fingerList);

		javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
		jPanel2.setLayout(jPanel2Layout);
		jPanel2Layout.setHorizontalGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel2Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jLabel8, javax.swing.GroupLayout.PREFERRED_SIZE, 118, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addComponent(jLabel9, javax.swing.GroupLayout.PREFERRED_SIZE, 118, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addComponent(jLabel10, javax.swing.GroupLayout.PREFERRED_SIZE, 118, javax.swing.GroupLayout.PREFERRED_SIZE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel2Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 614, Short.MAX_VALUE)
										.addComponent(jScrollPane2, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE,
												614, Short.MAX_VALUE)
										.addComponent(predecessorField, javax.swing.GroupLayout.Alignment.TRAILING,
												javax.swing.GroupLayout.DEFAULT_SIZE, 614, Short.MAX_VALUE)).addContainerGap()));
		jPanel2Layout.setVerticalGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel2Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addComponent(jLabel8)
										.addComponent(predecessorField))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 17, Short.MAX_VALUE)
										.addComponent(jLabel9))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addComponent(jLabel10)
										.addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 183, Short.MAX_VALUE)).addContainerGap()));

		jPanel2Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] { jLabel10, jLabel9, jScrollPane2, predecessorField });

		tabbedPane.addTab("Routing", jPanel2);

		logPropertiesArea.setColumns(20);
		logPropertiesArea.setRows(5);
		jScrollPane3.setViewportView(logPropertiesArea);

		jButton2.setText("Update");
		jButton2.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				updateLogProperties(evt);
			}
		});

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3.setLayout(jPanel3Layout);
		jPanel3Layout.setHorizontalGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel3Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel3Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
										.addComponent(jScrollPane3, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												744, Short.MAX_VALUE).addComponent(jButton2)).addContainerGap()));
		jPanel3Layout.setVerticalGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel3Layout.createSequentialGroup().addContainerGap()
						.addComponent(jScrollPane3, javax.swing.GroupLayout.DEFAULT_SIZE, 200, Short.MAX_VALUE)
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED).addComponent(jButton2).addContainerGap()));

		tabbedPane.addTab("Log", jPanel3);

		profileTable.setAutoCreateRowSorter(true);
		profileTable.setModel(profileTableModel);
		profileTable.setAutoResizeMode(javax.swing.JTable.AUTO_RESIZE_ALL_COLUMNS);
		profileTable.setEnabled(false);
		jScrollPane4.setViewportView(profileTable);

		jButton3.setText("Reset");
		jButton3.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				resetDHashTimeKeeper(evt);
			}
		});

		jLabel11.setText("Network load");

		networkLoadLabel.setText("0");

		jLabel12.setText("Task load");

		taskLoadLabel.setText("0");

		jLabel16.setText("Queue");

		queueLabel.setText("0");

		javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
		jPanel4.setLayout(jPanel4Layout);
		jPanel4Layout.setHorizontalGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel4Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel4Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
										.addComponent(jScrollPane4, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												744, Short.MAX_VALUE)
										.addGroup(
												jPanel4Layout.createSequentialGroup().addComponent(jLabel11)
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
														.addComponent(networkLoadLabel)
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED).addComponent(jLabel12)
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
														.addComponent(taskLoadLabel)
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED).addComponent(jLabel16)
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED).addComponent(queueLabel)
														.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 461, Short.MAX_VALUE)
														.addComponent(jButton3))).addContainerGap()));
		jPanel4Layout.setVerticalGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel4Layout
						.createSequentialGroup()
						.addContainerGap()
						.addComponent(jScrollPane4, javax.swing.GroupLayout.DEFAULT_SIZE, 200, Short.MAX_VALUE)
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE).addComponent(jButton3)
										.addComponent(jLabel11).addComponent(networkLoadLabel).addComponent(jLabel12).addComponent(taskLoadLabel)
										.addComponent(jLabel16).addComponent(queueLabel)).addContainerGap()));

		tabbedPane.addTab("Profile", jPanel4);

		executeField.setFont(new java.awt.Font("Monospaced", 0, 11));
		executeField.addKeyListener(new java.awt.event.KeyAdapter() {
			public void keyPressed(java.awt.event.KeyEvent evt) {
				executeFieldKeyPressed(evt);
			}
		});

		responseArea.setColumns(20);
		responseArea.setEditable(false);
		responseArea.setFont(new java.awt.Font("Monospaced", 0, 11));
		responseArea.setRows(5);
		jScrollPane5.setViewportView(responseArea);

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5.setLayout(jPanel5Layout);
		jPanel5Layout.setHorizontalGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel5Layout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								jPanel5Layout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
										.addComponent(jScrollPane5, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												744, Short.MAX_VALUE)
										.addComponent(executeField, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												744, Short.MAX_VALUE)).addContainerGap()));
		jPanel5Layout.setVerticalGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel5Layout
						.createSequentialGroup()
						.addContainerGap()
						.addComponent(jScrollPane5, javax.swing.GroupLayout.DEFAULT_SIZE, 206, Short.MAX_VALUE)
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addComponent(executeField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE,
								javax.swing.GroupLayout.PREFERRED_SIZE).addContainerGap()));

		tabbedPane.addTab("Shell", jPanel5);

		jLabel15.setText("Entries held");

		jLabel17.setText("Entries owned");

		jLabel19.setText("Oldest entry");

		jLabel21.setText("Youngest entry");

		javax.swing.GroupLayout dataPanelLayout = new javax.swing.GroupLayout(dataPanel);
		dataPanel.setLayout(dataPanelLayout);
		dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				dataPanelLayout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								dataPanelLayout
										.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
										.addComponent(jLabel21, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(jLabel19, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(jLabel17, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(jLabel15, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(entriesOwnedField, javax.swing.GroupLayout.DEFAULT_SIZE, 650, Short.MAX_VALUE)
										.addComponent(entriesHeldField, javax.swing.GroupLayout.DEFAULT_SIZE, 650, Short.MAX_VALUE)
										.addComponent(oldestEntryField, javax.swing.GroupLayout.DEFAULT_SIZE, 650, Short.MAX_VALUE)
										.addComponent(youngestEntryField, javax.swing.GroupLayout.DEFAULT_SIZE, 650, Short.MAX_VALUE))
						.addContainerGap()));
		dataPanelLayout.setVerticalGroup(dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				dataPanelLayout
						.createSequentialGroup()
						.addContainerGap()
						.addGroup(
								dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE).addComponent(entriesHeldField)
										.addComponent(jLabel15))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE).addComponent(jLabel17)
										.addComponent(entriesOwnedField))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE).addComponent(jLabel19)
										.addComponent(oldestEntryField))
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addGroup(
								dataPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE).addComponent(jLabel21)
										.addComponent(youngestEntryField)).addContainerGap(171, Short.MAX_VALUE)));

		tabbedPane.addTab("Data", dataPanel);

		waitingTable.setAutoCreateRowSorter(true);
		waitingTable.setModel(waitingCommandsTableModel);
		jScrollPane6.setViewportView(waitingTable);

		javax.swing.GroupLayout jPanel7Layout = new javax.swing.GroupLayout(jPanel7);
		jPanel7.setLayout(jPanel7Layout);
		jPanel7Layout.setHorizontalGroup(jPanel7Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel7Layout.createSequentialGroup().addContainerGap()
						.addComponent(jScrollPane6, javax.swing.GroupLayout.DEFAULT_SIZE, 744, Short.MAX_VALUE).addContainerGap()));
		jPanel7Layout.setVerticalGroup(jPanel7Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel7Layout.createSequentialGroup().addContainerGap()
						.addComponent(jScrollPane6, javax.swing.GroupLayout.DEFAULT_SIZE, 229, Short.MAX_VALUE).addContainerGap()));

		tabbedPane.addTab("Waiting", jPanel7);

		monitorSwitch.setText("Send start");
		monitorSwitch.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				switchMonitoring(evt);
			}
		});

		javax.swing.GroupLayout monitorPanelLayout = new javax.swing.GroupLayout(monitorPanel);
		monitorPanel.setLayout(monitorPanelLayout);
		monitorPanelLayout.setHorizontalGroup(monitorPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				monitorPanelLayout.createSequentialGroup().addContainerGap(662, Short.MAX_VALUE).addComponent(monitorSwitch).addContainerGap()));
		monitorPanelLayout.setVerticalGroup(monitorPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				monitorPanelLayout.createSequentialGroup().addContainerGap().addComponent(monitorSwitch).addContainerGap(218, Short.MAX_VALUE)));

		tabbedPane.addTab("Monitor", monitorPanel);

		jMenu1.setText("File");

		jMenuItem1.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_R, java.awt.event.InputEvent.CTRL_MASK));
		jMenuItem1.setText("Reconnect");
		jMenuItem1.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				showReconnect(evt);
			}
		});
		jMenu1.add(jMenuItem1);

		jMenuItem2.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_Q, java.awt.event.InputEvent.CTRL_MASK));
		jMenuItem2.setText("Exit");
		jMenuItem2.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				exit(evt);
			}
		});
		jMenu1.add(jMenuItem2);

		jMenuBar1.add(jMenu1);

		setJMenuBar(jMenuBar1);

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
		getContentPane().setLayout(layout);
		layout.setHorizontalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addComponent(tabbedPane,
				javax.swing.GroupLayout.DEFAULT_SIZE, 773, Short.MAX_VALUE));
		layout.setVerticalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addComponent(tabbedPane,
				javax.swing.GroupLayout.DEFAULT_SIZE, 278, Short.MAX_VALUE));

		pack();
	}// </editor-fold>//GEN-END:initComponents

	private void reconnect(InetSocketAddress address) {
		try {
			if (proxy instanceof RemoteChordProxy) {
				proxy = new RemoteChordProxy(address);
			}
			setupTimer();
			refreshParameters = true;
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, "Error reconnecting: " + e);
			return;
		}
	}

	private void restartChord(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_restartChord
		InetSocketAddress local = null;
		try {
			local = new InetSocketAddress(localAddressField.getText(), Integer.parseInt(localPortField.getText()));
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, "The local address and port are invalid: " + e);
			return;
		}
		InetSocketAddress bootstrap = null;
		if (bootstrapAddressField.getText().length() > 0) {
			try {
				bootstrap = new InetSocketAddress(bootstrapAddressField.getText(), Integer.parseInt(bootstrapPortField.getText()));
			} catch (Exception e) {
				JOptionPane.showMessageDialog(this, "The bootstrap address and port are invalid: " + e);
				return;
			}
		}
		Pattern p = Pattern.compile("\\[(.*)\\]");
		Matcher m = p.matcher(storageField.getText());
		String[] storageDescription = new String[0];
		if (m.matches()) {
			storageDescription = m.group(1).replaceAll("\\s", "").split(",");
		} else {
			JOptionPane.showMessageDialog(this, "The Storage description is invalid. It should look like '[KLASS, ARG1, ARG2 ... ARGN]'");
			return;
		}
		Identifier identifier;
		try {
			identifier = new Identifier(identifierField.getText());
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, "The identifier is invalid: " + e);
			return;
		}
		int copies;
		try {
			copies = Integer.parseInt(copiesField.getText());
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, "The number of copies is not an integer: " + e);
			return;
		}
		timer.cancel();
		try {
			Object restartResponse = getProxy().run(
					new DHashRestartCommand(null, local, bootstrap, identifier, copies, storageDescription, serviceNameField.getText()));
			if (restartResponse instanceof RuntimeException) {
				((Throwable) restartResponse).printStackTrace();
				JOptionPane.showMessageDialog(this, "Error restarting: " + restartResponse);
				return;
			}
		} catch (RuntimeException e) {
			if (e.getCause() instanceof EOFException) {
			} else {
				e.printStackTrace();
				JOptionPane.showMessageDialog(this, "Error restarting: " + e);
				return;
			}
		}
		reconnect(local);
	}// GEN-LAST:event_restartChord

	private void updateLogProperties(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_updateLogProperties
		Object updateLogResponse = getProxy().run(new LogPropertiesCommand(null, logPropertiesArea.getText()));
		if (updateLogResponse instanceof RuntimeException) {
			JOptionPane.showMessageDialog(this, "Error setting log properties: " + updateLogResponse);
		}
	}// GEN-LAST:event_updateLogProperties

	private void resetDHashTimeKeeper(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_resetDHashTimeKeeper
		Object resetResponse = getProxy().run(new TimeKeeperResetCommand(null));
		if (resetResponse instanceof RuntimeException) {
			JOptionPane.showMessageDialog(this, "Error resetting timer: " + resetResponse);
		}
	}// GEN-LAST:event_resetDHashTimeKeeper

	private void executeFieldKeyPressed(java.awt.event.KeyEvent evt) {// GEN-FIRST:event_executeFieldKeyPressed
		switch (evt.getKeyCode()) {
		case KeyEvent.VK_ENTER:
			if (executeField.getText().trim().length() > 0) {
				scriptTracker.eval(executeField.getText());
			}
			break;
		case KeyEvent.VK_UP:
			scriptTracker.historyBack();
			break;
		case KeyEvent.VK_DOWN:
			scriptTracker.historyForward();
			break;
		}
	}// GEN-LAST:event_executeFieldKeyPressed

	private void exit(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_exit
		System.exit(0);
	}// GEN-LAST:event_exit

	private void showReconnect(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_showReconnect
		reconnectDialog.pack();
		reconnectDialog.setVisible(true);
	}// GEN-LAST:event_showReconnect

	private void reconnect(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_reconnect
		InetSocketAddress newAddress = null;
		try {
			newAddress = new InetSocketAddress(reconnectAddressField.getText(), Integer.parseInt(reconnectPortField.getText()));
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, "The address and port are invalid: " + e);
			return;
		}
		timer.cancel();
		reconnect(newAddress);
		reconnectDialog.setVisible(false);
	}// GEN-LAST:event_reconnect

	private void tabChanged(javax.swing.event.ChangeEvent evt) {// GEN-FIRST:event_tabChanged
		JTabbedPane pane = (JTabbedPane) evt.getSource();
		if (pane.getSelectedComponent() == monitorPanel) {
			monitorPanel.startListening();
		} else {
			monitorPanel.stopListening();
		}
	}// GEN-LAST:event_tabChanged

	private void switchMonitoring(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_switchMonitoring
		if (monitoring) {
			monitoring = false;
			monitorSwitch.setText("Send start");
			monitorPanel.send(false);
		} else {
			monitoring = true;
			monitorSwitch.setText("Send stop");
			monitorPanel.send(true);
		}
	}// GEN-LAST:event_switchMonitoring

	private void serviceNameFieldActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_serviceNameFieldActionPerformed
		// TODO add your handling code here:
	}// GEN-LAST:event_serviceNameFieldActionPerformed

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JTextField bootstrapAddressField;

	private javax.swing.JTextField bootstrapPortField;

	private javax.swing.JTextField copiesField;

	private javax.swing.JPanel dataPanel;

	private javax.swing.JLabel entriesHeldField;

	private javax.swing.JLabel entriesOwnedField;

	private javax.swing.JTextField executeField;

	private javax.swing.JList fingerList;

	private javax.swing.JTextField identifierField;

	private javax.swing.JButton jButton1;

	private javax.swing.JButton jButton2;

	private javax.swing.JButton jButton3;

	private javax.swing.JButton jButton4;

	private javax.swing.JLabel jLabel1;

	private javax.swing.JLabel jLabel10;

	private javax.swing.JLabel jLabel11;

	private javax.swing.JLabel jLabel12;

	private javax.swing.JLabel jLabel13;

	private javax.swing.JLabel jLabel14;

	private javax.swing.JLabel jLabel15;

	private javax.swing.JLabel jLabel16;

	private javax.swing.JLabel jLabel17;

	private javax.swing.JLabel jLabel18;

	private javax.swing.JLabel jLabel19;

	private javax.swing.JLabel jLabel2;

	private javax.swing.JLabel jLabel21;

	private javax.swing.JLabel jLabel3;

	private javax.swing.JLabel jLabel4;

	private javax.swing.JLabel jLabel5;

	private javax.swing.JLabel jLabel6;

	private javax.swing.JLabel jLabel7;

	private javax.swing.JLabel jLabel8;

	private javax.swing.JLabel jLabel9;

	private javax.swing.JMenu jMenu1;

	private javax.swing.JMenuBar jMenuBar1;

	private javax.swing.JMenuItem jMenuItem1;

	private javax.swing.JMenuItem jMenuItem2;

	private javax.swing.JPanel jPanel1;

	private javax.swing.JPanel jPanel2;

	private javax.swing.JPanel jPanel3;

	private javax.swing.JPanel jPanel4;

	private javax.swing.JPanel jPanel5;

	private javax.swing.JPanel jPanel7;

	private javax.swing.JScrollPane jScrollPane1;

	private javax.swing.JScrollPane jScrollPane2;

	private javax.swing.JScrollPane jScrollPane3;

	private javax.swing.JScrollPane jScrollPane4;

	private javax.swing.JScrollPane jScrollPane5;

	private javax.swing.JScrollPane jScrollPane6;

	private javax.swing.JTextField localAddressField;

	private javax.swing.JTextField localPortField;

	private javax.swing.JTextArea logPropertiesArea;

	private cx.ath.troja.chordless.gui.MonitorPanel monitorPanel;

	private javax.swing.JButton monitorSwitch;

	private javax.swing.JLabel networkLoadLabel;

	private javax.swing.JLabel oldestEntryField;

	private javax.swing.JTextField predecessorField;

	private javax.swing.JTable profileTable;

	private javax.swing.JLabel queueLabel;

	private javax.swing.JTextField reconnectAddressField;

	private javax.swing.JDialog reconnectDialog;

	private javax.swing.JTextField reconnectPortField;

	private javax.swing.JTextArea responseArea;

	private javax.swing.JTextField serviceNameField;

	private javax.swing.JTextField storageField;

	private javax.swing.JList successorList;

	private javax.swing.JTabbedPane tabbedPane;

	private javax.swing.JLabel taskLoadLabel;

	private javax.swing.JTable waitingTable;

	private javax.swing.JLabel youngestEntryField;
	// End of variables declaration//GEN-END:variables

}
