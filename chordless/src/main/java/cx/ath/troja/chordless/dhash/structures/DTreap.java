/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.structures;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Delays;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public class DTreap<K extends Comparable<? super K>, V> extends Persistent implements DMap<K, V> {

	private static final long serialVersionUID = 2L;

	private static Random random = new Random();

	private static <K extends Comparable<? super K>> int cmp(K k1, K k2) {
		if (k1 == k2) {
			return 0;
		} else if (k1 == null) {
			return -1;
		} else if (k2 == null) {
			return 1;
		} else {
			return k1.compareTo(k2);
		}
	}

	private static String indent(int l) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < l; i++) {
			b.append(" ");
		}
		return b.toString();
	}

	public static abstract class Inject<S, K extends Comparable<? super K>, V> extends DMap.DefaultDInject<S, K, V> {
		private boolean done = false;

		public void done() {
			done = true;
		}

		public boolean isDone() {
			return done;
		}
	}

	private static class InjectEnvoy<S, K extends Comparable<? super K>, V> extends Persister.Envoy<Node<K, V>> {
		private DMap.DInject<S, K, V> inject;

		private S s;

		private StepEnvoy.Direction direction;

		public InjectEnvoy(S s, DMap.DInject<S, K, V> inject, StepEnvoy.Direction direction) {
			this.inject = inject;
			this.s = s;
			this.direction = direction;
		}

		public void handle(Node<K, V> node) {
			Delay<Identifier> d = node.envoy(new StepEnvoy<K>(direction), node.getIdentifier());
			d.get();
			s = inject.perform(s, node, node);
			if (d.get() == null || (inject instanceof Inject && ((Inject) inject).isDone())) {
				returnHome(s);
			} else {
				redirect(d.get());
			}
		}
	}

	public static class RotateEnvoy<K extends Comparable<? super K>, V> extends Persister.Envoy<Node<K, V>> {
		private NodeData child;

		public RotateEnvoy(NodeData child) {
			this.child = child;
		}

		/*
		 * Will replace the child attribute at time of rotation. Rotate returns the replacement node as it looks
		 * BEFORE rotation, so parent, and right will NOT be correct. However, since we only use the priority and identifier
		 * of the child attribute, this should be fine.
		 */
		public void handle(Node<K, V> node) {
			if (child.priority > node.getPriority()) {
				Identifier parentBefore = node.getParent();
				if (node.getLeft() != null && child.identifier.equals(node.getLeft().identifier)) {
					node.rotateRight();
				} else if (node.getRight() != null && child.identifier.equals(node.getRight().identifier)) {
					node.rotateLeft();
				} else {
					throw new RuntimeException("" + node + " has no child " + child);
				}
				if (parentBefore != null) {
					redirect(parentBefore);
				} else {
					returnHome(null);
				}
			} else {
				returnHome(null);
			}
		}
	}

	public static class InsertEnvoy<K extends Comparable<? super K>, V> extends Persister.Envoy<Node<K, V>> {
		private K k;

		private V v;

		public InsertEnvoy(K k, V v) {
			this.k = k;
			this.v = v;
		}

		public void handle(Node<K, V> node) {
			int i = cmp(node.getKey(), k);
			if (i > 0) {
				if (node.getLeft() == null) {
					NodeData newLeft = node.insertLeft(k, v);
					if (newLeft.priority > node.getPriority()) {
						node.envoy(new RotateEnvoy<K, V>(newLeft), node.getIdentifier()).get();
					}
					returnHome(null);
				} else {
					redirect(node.getLeft().identifier);
				}
			} else if (i < 0) {
				if (node.getRight() == null) {
					NodeData newRight = node.insertRight(k, v);
					if (newRight.priority > node.getPriority()) {
						node.envoy(new RotateEnvoy<K, V>(newRight), node.getIdentifier()).get();
					}
					returnHome(null);
				} else {
					redirect(node.getRight().identifier);
				}
			} else {
				V returnValue = node.getValue();
				node.setValue(v);
				returnHome(returnValue);
			}
		}
	}

	public static class GetEnvoy<K extends Comparable<? super K>, V> extends Persister.Envoy<Node<K, V>> {
		private K k;

		public GetEnvoy(K k) {
			this.k = k;
		}

		public void handle(Node<K, V> node) {
			int i = cmp(node.getKey(), k);
			if (i > 0) {
				if (node.getLeft() == null) {
					returnHome(null);
				} else {
					redirect(node.getLeft().identifier);
				}
			} else if (i < 0) {
				if (node.getRight() == null) {
					returnHome(null);
				} else {
					redirect(node.getRight().identifier);
				}
			} else {
				act(node);
			}
		}

		protected void act(Node<K, V> node) {
			returnHome(node.getValue());
		}
	}

	public static class RemoveEnvoy<K extends Comparable<? super K>, V> extends GetEnvoy<K, V> {
		public RemoveEnvoy(K k) {
			super(k);
		}

		@Override
		protected void act(Node<K, V> node) {
			node.remove();
			returnHome(node.getValue());
		}
	}

	public static class GetClosestEnvoy<K extends Comparable<? super K>, V> extends Persister.Envoy<Node<K, V>> {
		private enum State {
			FIND, FIND_BELOW, FIND_ABOVE
		}

		private K k;

		private StepEnvoy.Direction direction;

		private State state;

		public GetClosestEnvoy(K k, StepEnvoy.Direction direction) {
			this.k = k;
			this.direction = direction;
			this.state = State.FIND;
		}

		public void handle(Node<K, V> node) {
			int i = cmp(node.getKey(), k);
			if (state == State.FIND) {
				if (i > 0) {
					if (node.getLeft() == null) {
						if (direction == StepEnvoy.Direction.FORWARD) {
							returnHome(node.getData());
						} else {
							state = State.FIND_BELOW;
							Delay<Identifier> previous = node.envoy(new StepEnvoy<K>(StepEnvoy.Direction.BACKWARD), node.getIdentifier());
							redirect(previous.get());
						}
					} else {
						redirect(node.getLeft().identifier);
					}
				} else if (i < 0) {
					if (node.getRight() == null) {
						if (direction == StepEnvoy.Direction.BACKWARD) {
							returnHome(node.getData());
						} else {
							state = State.FIND_ABOVE;
							Delay<Identifier> next = node.envoy(new StepEnvoy<K>(StepEnvoy.Direction.FORWARD), node.getIdentifier());
							redirect(next.get());
						}
					} else {
						redirect(node.getRight().identifier);
					}
				} else {
					returnHome(node.getData());
				}
			} else if (state == State.FIND_BELOW) {
				if (i < 0) {
					returnHome(node.getData());
				} else if (i > 0) {
					Delay<Identifier> previous = node.envoy(new StepEnvoy<K>(StepEnvoy.Direction.BACKWARD), node.getIdentifier());
					redirect(previous.get());
				} else {
					returnHome(node.getData());
				}
			} else if (state == State.FIND_ABOVE) {
				if (i > 0) {
					returnHome(node.getData());
				} else if (i < 0) {
					Delay<Identifier> next = node.envoy(new StepEnvoy<K>(StepEnvoy.Direction.FORWARD), node.getIdentifier());
					redirect(next.get());
				} else {
					returnHome(node.getData());
				}
			}
		}
	}

	public static class StepEnvoy<K extends Comparable<? super K>> extends Persister.Envoy<Node<K, Object>> {
		private enum State {
			STEP_UP, FIRST_FORWARD, DEEP_BACKWARD
		}

		public enum Direction {
			FORWARD, BACKWARD
		};

		private State state;

		private K last;

		private Direction direction;

		public StepEnvoy(Direction direction) {
			this.state = State.FIRST_FORWARD;
			this.last = null;
			this.direction = direction;
		}

		public void handle(Node<K, Object> node) {
			NodeData vForward = null;
			NodeData vBackward = null;
			if (direction == Direction.BACKWARD) {
				vForward = node.getLeft();
				vBackward = node.getRight();
			} else {
				vForward = node.getRight();
				vBackward = node.getLeft();
			}
			if (state == State.FIRST_FORWARD) {
				if (vForward == null) {
					if (node.getParent() == null) {
						returnHome(null);
					} else {
						last = node.getKey();
						state = State.STEP_UP;
						redirect(node.getParent());
					}
				} else {
					state = State.DEEP_BACKWARD;
					redirect(vForward.identifier);
				}
			} else if (state == State.DEEP_BACKWARD) {
				if (vBackward == null) {
					returnHome(node.getIdentifier());
				} else {
					redirect(vBackward.identifier);
				}
			} else if (state == State.STEP_UP) {
				int i = 0;
				if (direction == Direction.BACKWARD) {
					i = cmp(node.getKey(), last);
				} else {
					i = cmp(last, node.getKey());
				}
				if (i < 0) {
					returnHome(node.getIdentifier());
				} else {
					if (node.getParent() == null) {
						returnHome(null);
					} else {
						redirect(node.getParent());
					}
				}
			}
		}
	}

	public static class DTreapIterator<K extends Comparable<? super K>, V> implements DMap.DEntryIterator<K, V> {
		private Identifier treap;

		private Identifier location;

		private Identifier previous;

		private Identifier last;

		public DTreapIterator(Persister p, Identifier treap, Identifier location) {
			this.treap = treap;
			if (location != null) {
				Delay<Identifier> d = p.envoy(new StepEnvoy<K>(StepEnvoy.Direction.BACKWARD), location);
				previous = d.get();
			}
			this.location = location;
			this.last = null;
		}

		@Override
		public boolean hasNext(Persister p) {
			return location != null;
		}

		@Override
		public boolean hasPrevious(Persister p) {
			return previous != null;
		}

		@Override
		public Map.Entry<K, V> next(Persister p) {
			if (location == null) {
				throw new RuntimeException("You can not call next on a DTreapIterator that doesn't have a next");
			} else {
				Delay<Map.Entry<K, V>> returnValue = p.exec(location, "getMapEntry");
				Delay<Identifier> d = p.envoy(new StepEnvoy<K>(StepEnvoy.Direction.FORWARD), location);
				previous = location;
				last = location;
				location = d.get();
				return returnValue.get();
			}
		}

		@Override
		public Map.Entry<K, V> previous(Persister p) {
			if (previous == null) {
				throw new RuntimeException("You can not call previous on a DTreapIterator that doesn't have a previous");
			} else {
				Delay<Map.Entry<K, V>> returnValue = p.exec(previous, "getMapEntry");
				last = previous;
				location = previous;
				Delay<Identifier> d = p.envoy(new StepEnvoy<K>(StepEnvoy.Direction.BACKWARD), previous);
				previous = d.get();
				return returnValue.get();
			}
		}

		@Override
		public Identifier current(Persister p) {
			return location;
		}

		@Override
		public void remove(Persister p) {
			if (last == null) {
				throw new RuntimeException(
						"You can not remove from a DTreapIterator before you have fetched at least one entry from it since creation or last remove");
			} else {
				Delay<Identifier> d = p.envoy(new StepEnvoy<K>(StepEnvoy.Direction.BACKWARD), last);
				previous = d.get();
				d = p.envoy(new StepEnvoy<K>(StepEnvoy.Direction.FORWARD), last);
				location = d.get();
				p.exec(last, "remove").get();
				last = null;
			}
		}
	}

	public static class ClearInject<K extends Comparable<? super K>, V> extends DMap.DefaultDEach<K, V> {
		public void each(Map.Entry<K, V> e) {
			remove();
		}
	}

	public static class PrintEach<K extends Comparable<? super K>> extends DMap.DefaultDEach<K, Object> {
		public void each(Map.Entry<K, Object> e) {
			System.out.println("" + e.getKey() + " => " + e.getValue());
		}
	}

	private static class FirstEnvoy<K extends Comparable<? super K>, V> extends Envoy<Node<K, V>> {
		@Override
		public void handle(Node<K, V> n) {
			if (n.getLeft() == null) {
				returnHome(n.getData());
			} else {
				redirect(n.getLeft().identifier);

			}
		}
	}

	private static class LastEnvoy<K extends Comparable<? super K>, V> extends Envoy<Node<K, V>> {
		@Override
		public void handle(Node<K, V> n) {
			if (n.getRight() == null) {
				returnHome(n.getData());
			} else {
				redirect(n.getRight().identifier);
			}
		}
	}

	private static class NodeData implements Serializable {
		public Identifier identifier;

		public long priority;

		public NodeData(Identifier identifier, long priority) {
			this.identifier = identifier;
			this.priority = priority;
		}

		public String toString() {
			return "<id=" + identifier + " prio=" + priority + ">";
		}

		public boolean equals(Object o) {
			if (o instanceof NodeData) {
				NodeData other = (NodeData) o;
				return other.identifier.equals(identifier);
			} else {
				return false;
			}
		}
	}

	public static class Node<K extends Comparable<? super K>, V> extends DMap.DMapEntry<K, V> {
		private static final long serialVersionUID = 1L;

		private Identifier treap;

		private Identifier id;

		private NodeData left;

		private NodeData right;

		private Identifier parent;

		private long priority;

		public Node(Identifier t, Identifier p, K k, V v) {
			treap = t;
			id = Identifier.random();
			parent = p;
			left = null;
			right = null;
			key = k;
			value = v;
			priority = random.nextLong();
		}

		@Override
		public Object getId() {
			return id;
		}

		public long getPriority() {
			return priority;
		}

		public Identifier getParent() {
			return parent;
		}

		public NodeData getRight() {
			return right;
		}

		public NodeData getLeft() {
			return left;
		}

		public void setLeft(NodeData i) {
			left = i;
		}

		public void setRight(NodeData i) {
			right = i;
		}

		public void setParent(Identifier i) {
			parent = i;
		}

		public void setValue(V v) {
			value = v;
		}

		public void replaceChild(NodeData child, NodeData replacement) {
			if (child.equals(left)) {
				left = replacement;
			} else if (child.equals(right)) {
				right = replacement;
			} else {
				throw new RuntimeException("No child " + child + " for " + this);
			}
		}

		public String toString() {
			return "<" + getClass().getName() + " identifier=" + id + " parent=" + parent + " left=" + left + " right=" + right + " priority="
					+ priority + " key=" + key + " value=" + value + ">";
		}

		private String shortPrio() {
			return BigInteger.valueOf(priority).toString(16);
		}

		public void print(String indent) {
			System.out.println(getIdentifier() + " (priority " + shortPrio() + ", " + key + " => " + value + ")");
			if (left == null) {
				System.out.println(indent + "left -> null");
			} else {
				String l = indent + "left -> ";
				System.out.print(indent + "left -> ");
				exec(left.identifier, "print", indent(l.length())).get();
			}
			if (right == null) {
				System.out.println(indent + "right -> null");
			} else {
				String l = indent + "right -> ";
				System.out.print(indent + "right -> ");
				exec(right.identifier, "print", indent(l.length())).get();
			}
		}

		public NodeData getData() {
			return new NodeData(getIdentifier(), priority);
		}

		@Override
		public Delay<? extends Object> removeAsync() {
			while (left != null && right != null) {
				if (new Long(left.priority).compareTo(new Long(right.priority)) > 0) {
					rotateRight();
				} else {
					rotateLeft();
				}
			}

			Transaction t = transaction();
			Delays d = new Delays();

			if (parent == null) {
				if (left != null) {
					d.add(t.exec(left.identifier, "setParent", parent));
					d.add(t.exec(treap, "update", -1, true, left.identifier));
				} else if (right != null) {
					d.add(t.exec(right.identifier, "setParent", parent));
					d.add(t.exec(treap, "update", -1, true, right.identifier));
				} else {
					d.add(t.exec(treap, "update", -1, true, parent));
				}
			} else {
				if (left != null) {
					d.add(t.exec(left.identifier, "setParent", parent));
					d.add(t.exec(treap, "update", -1, false, null));
					d.add(t.exec(parent, "replaceChild", getData(), left));
				} else if (right != null) {
					d.add(t.exec(right.identifier, "setParent", parent));
					d.add(t.exec(treap, "update", -1, false, null));
					d.add(t.exec(parent, "replaceChild", getData(), right));
				} else {
					d.add(t.exec(treap, "update", -1, false, null));
					d.add(t.exec(parent, "replaceChild", getData(), null));
				}
			}

			d.get();
			t.commit();

			return super.removeAsync();
		}

		private void rotateRight() {
			Transaction t = transaction();
			Delays d = new Delays();

			Delay<NodeData> newLeft = t.exec(left.identifier, "getRight");
			newLeft.get();

			if (parent == null) {
				d.add(t.exec(treap, "update", 0, true, left.identifier));
			} else {
				d.add(t.exec(parent, "replaceChild", getData(), left));
			}
			d.add(t.exec(left.identifier, "setParent", parent));

			d.add(t.exec(left.identifier, "setRight", getData()));
			d.add(t.exec(getIdentifier(), "setParent", left.identifier));
			parent = left.identifier;

			d.add(t.exec(getIdentifier(), "setLeft", newLeft.get()));
			left = newLeft.get();
			if (newLeft.get() != null) {
				d.add(t.exec(newLeft.get().identifier, "setParent", getIdentifier()));
			}

			d.get();
			t.commit();
		}

		private void rotateLeft() {
			Transaction t = transaction();
			Delays d = new Delays();

			Delay<NodeData> newRight = t.exec(right.identifier, "getLeft");
			newRight.get();

			if (parent == null) {
				d.add(t.exec(treap, "update", 0, true, right.identifier));
			} else {
				d.add(t.exec(parent, "replaceChild", getData(), right));
			}
			d.add(t.exec(right.identifier, "setParent", parent));

			d.add(t.exec(right.identifier, "setLeft", getData()));
			d.add(t.exec(getIdentifier(), "setParent", right.identifier));
			parent = right.identifier;

			d.add(t.exec(getIdentifier(), "setRight", newRight.get()));
			right = newRight.get();
			if (newRight.get() != null) {
				d.add(t.exec(newRight.get().identifier, "setParent", getIdentifier()));
			}

			d.get();
			t.commit();
		}

		private NodeData insertRight(K k, V v) {
			if (right == null) {
				Transaction t = transaction();
				Delays d = new Delays();
				Node<K, V> node = new Node<K, V>(treap, getIdentifier(), k, v);
				d.add(t.put(node));
				d.add(t.exec(treap, "update", 1, false, null));
				d.get();
				t.commit();
				right = node.getData();
				return right;
			} else {
				throw new RuntimeException("" + this + " already has a right child");
			}
		}

		private NodeData insertLeft(K k, V v) {
			if (left == null) {
				Transaction t = transaction();
				Delays d = new Delays();
				Node<K, V> node = new Node<K, V>(treap, getIdentifier(), k, v);
				d.add(t.put(node));
				d.add(t.exec(treap, "update", 1, false, null));
				d.get();
				t.commit();
				left = node.getData();
				return left;
			} else {
				throw new RuntimeException("" + this + " already has a left child");
			}
		}

	}

	private Identifier id;

	private Identifier root;

	private long size;

	public DTreap() {
		id = Identifier.random();
		root = null;
		size = 0;
	}

	public void setId(Identifier x) {
		id = x;
	}

	public void setRoot(Identifier i) {
		root = i;
	}

	public Identifier getRoot() {
		return root;
	}

	@Override
	public long size() {
		return size;
	}

	public void update(Integer sizeDelta, Boolean useNewRoot, Identifier newRoot) {
		if (useNewRoot) {
			root = newRoot;
		}
		size += sizeDelta;
	}

	@Override
	public Object getId() {
		return id;
	}

	public void print() {
		if (root == null) {
			System.out.println("" + getIdentifier() + " (" + size + ") -> null");
		} else {
			String l = "" + getIdentifier() + " (" + size + ") -> ";
			System.out.print(l);
			exec(root, "print", indent(l.length())).get();
		}
	}

	public void clear() {
		inject(null, new ClearInject<K, V>(), 1, null);
		setTaint(false);
	}

	@Override
	protected Delay<? extends Object> removeAsync() {
		clear();
		return super.removeAsync();
	}

	@Override
	public void each(DMap.DEach<K, V> each) {
		inject(null, each);
	}

	@Override
	public <S> S inject(S s, DMap.DInject<S, K, V> inject) {
		return inject(s, inject, new Integer(1));
	}

	public void each(DMap.DEach<K, V> each, Integer direction) {
		inject(null, each, direction);
	}

	public <S> S inject(S s, DMap.DInject<S, K, V> inject, Integer direction) {
		return inject(s, inject, direction, null);
	}

	public void each(DMap.DEach<K, V> each, Integer direction, K start) {
		inject(null, each, direction, start);
	}

	public <S> S inject(S s, DMap.DInject<S, K, V> inject, Integer direction, K start) {
		setTaint(false);
		if (root == null) {
			return s;
		} else {
			Delay<NodeData> startDelay = null;
			if (start == null) {
				startDelay = envoy(direction < 0 ? new LastEnvoy<K, V>() : new FirstEnvoy<K, V>(), root);
			} else {
				startDelay = envoy(new GetClosestEnvoy<K, V>(start, direction < 0 ? StepEnvoy.Direction.BACKWARD : StepEnvoy.Direction.FORWARD), root);
			}
			Delay<S> delay = envoy(new InjectEnvoy<S, K, V>(s, inject, direction < 0 ? StepEnvoy.Direction.BACKWARD : StepEnvoy.Direction.FORWARD),
					startDelay.get().identifier);
			return delay.get();
		}
	}

	public Map.Entry<K, V> getFirst() {
		setTaint(false);
		if (root == null) {
			return null;
		} else {
			Delay<NodeData> dataDelay = envoy(new FirstEnvoy<K, V>(), root);
			Delay<Node<K, V>> nodeDelay = get(dataDelay.get().identifier);
			return nodeDelay.get().getMapEntry();
		}
	}

	public Map.Entry<K, V> getLast() {
		setTaint(false);
		if (root == null) {
			return null;
		} else {
			Delay<NodeData> dataDelay = envoy(new LastEnvoy<K, V>(), root);
			Delay<Node<K, V>> nodeDelay = get(dataDelay.get().identifier);
			return nodeDelay.get().getMapEntry();
		}
	}

	@Override
	public V get(K k) {
		setTaint(false);
		if (root == null) {
			return null;
		} else {
			Delay<V> delay = envoy(new GetEnvoy<K, V>(k), root);
			return delay.get();
		}
	}

	@Override
	public V del(K k) {
		setTaint(false);
		if (root == null) {
			return null;
		} else {
			Delay<V> delay = envoy(new RemoveEnvoy<K, V>(k), root);
			return delay.get();
		}
	}

	@Override
	public V put(K k, V v) {
		setTaint(false);
		if (root == null) {
			Node<K, V> n = new Node<K, V>(getIdentifier(), null, k, v);
			Transaction t = transaction();
			Delays delays = new Delays();
			delays.add(t.put(n));
			delays.add(t.exec(getIdentifier(), "update", new Integer(1), Boolean.TRUE, n.getIdentifier()));
			delays.get();
			t.commit();
			return null;
		} else {
			Delay<V> delay = envoy(new InsertEnvoy<K, V>(k, v), root);
			return delay.get();
		}
	}

	@Override
	public DTreapIterator<K, V> entryIterator() {
		if (root == null) {
			return entryIterator(null);
		} else {
			Delay<NodeData> startDelay = envoy(new FirstEnvoy<K, V>(), root);
			return entryIterator(startDelay.get().identifier);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public DTreapIterator<K, V> entryIterator(Identifier start) {
		return new DTreapIterator<K, V>(this, getIdentifier(), start);
	}

}