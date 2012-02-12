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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Delays;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public class DTree<T> extends Persistent implements DCollection.Primitive<T> {

	private static final long serialVersionUID = 1L;

	public static class RemoveAllEnvoy extends Persister.Envoy<Node> {
		public void handle(Node node) {
			Delay<Identifier> next = node.envoy(new NextEnvoy(), node.getIdentifier());
			next.get();
			node.remove();
			if (next.get() == null) {
				returnHome(null);
			} else {
				redirect(next.get());
			}
		}
	}

	private static class InjectEnvoy<V, T> extends Persister.Envoy<Node<T>> {
		private DCollection.DInject<V, T> inject;

		private V v;

		public InjectEnvoy(V v, DCollection.DInject<V, T> inject) {
			this.inject = inject;
			this.v = v;
		}

		public void handle(Node<T> node) {
			Delay<Identifier> d = node.envoy(new NextEnvoy(), node.getIdentifier());
			d.get();
			v = inject.perform(v, node, node);
			if (d.get() == null) {
				returnHome(v);
			} else {
				redirect(d.get());
			}
		}
	}

	public static class RemoveEnvoy<T> extends Persister.Envoy<Node> {
		private T value;

		public RemoveEnvoy(T t) {
			value = t;
		}

		public void handle(Node node) {
			if ((value == null && node.getValue() == null) || (value != null && value.equals(node.getValue()))) {

				node.remove();
				returnHome(null);
			} else {
				Delay<Identifier> d = node.envoy(new NextEnvoy(), node.getIdentifier());
				if (d.get() == null) {
					returnHome(null);
				} else {
					redirect(d.get());
				}
			}
		}
	}

	public static class ContainsEnvoy<T> extends Persister.Envoy<Node> {
		private T value;

		public ContainsEnvoy(T t) {
			value = t;
		}

		public void handle(Node node) {
			if ((value == null && node.getValue() == null) || (value != null && value.equals(node.getValue()))) {

				returnHome(Boolean.TRUE);
			} else {
				Delay<Identifier> d = node.envoy(new NextEnvoy(), node.getIdentifier());
				if (d.get() == null) {
					returnHome(Boolean.FALSE);
				} else {
					redirect(d.get());
				}
			}
		}
	}

	public static class PreviousEnvoy extends Persister.Envoy<Node> {
		private enum State {
			STEP_UP, FIRST_LEFT, DEEP_RIGHT
		}

		private State state;

		private Identifier last;

		public PreviousEnvoy() {
			this.state = State.STEP_UP;
			this.last = null;
		}

		public void handle(Node node) {
			NodeData<?> data = node.getData();
			if (state == State.STEP_UP) {
				if (data.parent == null) {
					returnHome(null);
				} else {
					state = State.FIRST_LEFT;
					last = data.identifier;
					redirect(data.parent);
				}
			} else if (state == State.FIRST_LEFT) {
				Identifier leftOf = data.children.lower(last);
				if (leftOf == null) {
					returnHome(data.identifier);
				} else {
					state = State.DEEP_RIGHT;
					redirect(data.children.last());
				}
			} else if (state == State.DEEP_RIGHT) {
				if (data.children.isEmpty()) {
					returnHome(data.identifier);
				} else {
					redirect(data.children.last());
				}
			}
		}
	}

	public static class NextEnvoy extends Persister.Envoy<Node> {
		private enum State {
			FIRST_CHILD, FIRST_RIGHT
		}

		private State state;

		private Identifier last;

		public NextEnvoy() {
			this.state = State.FIRST_CHILD;
			this.last = null;
		}

		public void handle(Node node) {
			NodeData<?> data = node.getData();
			if (state == State.FIRST_CHILD) {
				if (data.children.isEmpty()) {
					state = State.FIRST_RIGHT;
					last = data.identifier;
					if (data.parent == null) {
						returnHome(null);
					} else {
						redirect(data.parent);
					}
				} else {
					returnHome(data.children.first());
				}
			} else {
				Identifier rightOf = data.children.higher(last);
				if (rightOf == null) {
					if (data.parent == null) {
						returnHome(null);
					} else {
						last = data.identifier;
						redirect(data.parent);
					}
				} else {
					returnHome(rightOf);
				}
			}
		}
	}

	public static class DTreeIterator<T> implements DCollection.PrimitiveDIterator<T> {
		private Identifier tree;

		private Identifier location;

		private Identifier last;

		private Identifier previous;

		public DTreeIterator(Persister persister, Identifier tree, Identifier location) {
			this.tree = tree;
			if (location != null) {
				Delay<Identifier> d = persister.envoy(new PreviousEnvoy(), location);
				previous = d.get();
			}
			this.location = location;
			this.last = null;
		}

		@Override
		public Identifier getLast() {
			return last;
		}

		public boolean hasNext(Persister p) {
			return location != null;
		}

		public boolean hasPrevious(Persister p) {
			return previous != null;
		}

		public T next(Persister p) {
			if (location == null) {
				throw new RuntimeException("You can not call next on a DTreeIterator that doesn't have a next");
			} else {
				Delay<T> returnValue = p.exec(location, "getValue");
				Delay<Identifier> d = p.envoy(new NextEnvoy(), location);
				previous = location;
				last = location;
				location = d.get();
				return returnValue.get();
			}
		}

		public T previous(Persister p) {
			if (previous == null) {
				throw new RuntimeException("You can not call previous on a DTreeIterator that doesn't have a previous");
			} else {
				Delay<T> returnValue = p.exec(previous, "getValue");
				last = previous;
				location = previous;
				Delay<Identifier> d = p.envoy(new PreviousEnvoy(), previous);
				previous = d.get();
				return returnValue.get();
			}
		}

		public Identifier current(Persister p) {
			return location;
		}

		public void remove(Persister p) {
			if (last == null) {
				throw new RuntimeException(
						"You can not remove from a DTreeIterator before you have fetched at least one entry from it since creation or last remove");
			} else {
				Delay<Identifier> d = p.envoy(new PreviousEnvoy(), last);
				previous = d.get();
				d = p.envoy(new NextEnvoy(), last);
				location = d.get();
				p.exec(last, "remove").get();
				last = null;
			}
		}

		public void add(Persister p, T t) {
			p.exec(tree, "add", t).get();
		}
	}

	private static String indent(int l) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < l; i++) {
			b.append(" ");
		}
		return b.toString();
	}

	public static class NodeData<T> implements Serializable {
		public Identifier parent;

		public Identifier identifier;

		public T value;

		public ConcurrentSkipListSet<Identifier> children;

		public NodeData(Identifier identifier, T value, Identifier parent, ConcurrentSkipListSet<Identifier> children) {
			this.identifier = identifier;
			this.value = value;
			this.parent = parent;
			this.children = children;
		}

		@Override
		public String toString() {
			return "<" + getClass().getName() + " id=" + identifier + " parent=" + parent + " value=" + value + " children=" + children + ">";
		}
	}

	public static class Node<T> extends DCollection.DElement<T> {
		private static final long serialVersionUID = 1L;

		private Identifier tree;

		private Identifier parent;

		private ConcurrentSkipListSet<Identifier> children;

		public Node(T v) {
			id = Identifier.random();
			tree = null;
			parent = null;
			children = new ConcurrentSkipListSet<Identifier>();
			value = v;
		}

		private void setTree(Identifier i) {
			tree = i;
		}

		public NodeData<T> getData() {
			return new NodeData<T>(id, value, parent, children);
		}

		@Override
		public String toString() {
			return "<" + getClass().getName() + " id=" + id + " tree=" + tree + " parent=" + parent + " children=" + children + " value=" + value
					+ ">";
		}

		public T setValue(T v) {
			T oldValue = value;
			value = v;
			return oldValue;
		}

		public void print(String indent) {
			System.out.print(indent + getIdentifier() + " (" + value + ") -> ");
			if (children.isEmpty()) {
				System.out.println("null");
			} else {
				System.out.println();
				for (Identifier i : children) {
					exec(i, "print", indent + " ").get();
				}
			}
		}

		public void removeChild(Identifier i) {
			children.remove(i);
		}

		public void addChild(Identifier i) {
			children.add(i);
		}

		public void setParent(Identifier i) {
			parent = i;
		}

		@Override
		public Delay<? extends Object> removeAsync() {
			Delays delays = new Delays();
			Transaction t = transaction();
			if (parent == null) {
				if (children.isEmpty()) {
					delays.add(t.exec(tree, "update", new Integer(-1), true, null, true, null));
				} else {
					Identifier newRoot = children.pollFirst();
					delays.add(t.exec(newRoot, "setParent", new Object[] { null }));
					delays.add(t.exec(tree, "update", new Integer(-1), true, newRoot, true, newRoot));
					for (Identifier i : children) {
						delays.add(t.exec(i, "setParent", newRoot));
						delays.add(t.exec(newRoot, "addChild", i));
					}
				}
			} else {
				delays.add(t.exec(tree, "update", new Integer(-1), false, null, true, parent));
				delays.add(t.exec(parent, "removeChild", getIdentifier()));
				for (Identifier i : children) {
					delays.add(t.exec(i, "setParent", parent));
					delays.add(t.exec(parent, "addChild", i));
				}
			}
			delays.get();
			t.commit();
			return super.removeAsync();
		}

	}

	private Identifier id;

	private Identifier root;

	private AtomicLong size;

	private Identifier lastAddition;

	public DTree() {
		id = Identifier.random();
		root = null;
		size = new AtomicLong(0);
		lastAddition = null;
	}

	public void setId(Identifier x) {
		id = x;
	}

	@Override
	public long size() {
		return size.get();
	}

	@Override
	public String toString() {
		return "<" + getClass().getName() + " id=" + id + " root=" + root + " size=" + size + " lastAddition=" + lastAddition + ">";
	}

	public void update(Integer sizeDelta, Boolean useNewRoot, Identifier newRoot, Boolean useNewLastAddition, Identifier lastAddition) {
		if (useNewRoot) {
			root = newRoot;
		}
		if (useNewLastAddition) {
			this.lastAddition = lastAddition;
		}
		size.getAndAdd(sizeDelta.longValue());
	}

	@Override
	public Object getId() {
		return id;
	}

	public void print() {
		if (root == null) {
			System.out.println("" + getIdentifier() + " -> null");
		} else {
			String l = "" + getIdentifier() + " -> ";
			System.out.println(l);
			exec(root, "print", indent(l.length())).get();
		}
	}

	@Override
	public DTreeIterator<T> iterator() {
		return new DTreeIterator<T>(this, getIdentifier(), root);
	}

	@Override
	public DTreeIterator<T> iterator(Identifier start) {
		return new DTreeIterator<T>(this, getIdentifier(), start);
	}

	@Override
	protected Delay<? extends Object> removeAsync() {
		clear();
		return super.removeAsync();
	}

	private Delay<? extends Object> clearAsync() {
		if (root == null) {
			return new Delay<Object>() {
				public Object get(long timeout) {
					return get();
				}

				public Object get() {
					return null;
				}
			};
		} else {
			return envoy(new RemoveAllEnvoy(), root);
		}
	}

	public void clear() {
		clearAsync().get();
		setTaint(false);
	}

	@Override
	public void remove(T t) {
		if (root != null) {
			envoy(new RemoveEnvoy<T>(t), root).get();
		}
	}

	@Override
	public boolean contains(T t) {
		if (root == null) {
			return false;
		} else {
			Delay<Boolean> d = envoy(new ContainsEnvoy<T>(t), root);
			return d.get();
		}
	}

	public Identifier addNode(Node<T> node) {
		setTaint(false);
		node.setTree(getIdentifier());
		if (root == null) {
			Transaction t = transaction();
			Delays delays = new Delays();
			node.setParent(null);
			delays.add(t.put(node));
			delays.add(t.commute(getIdentifier(), "update", new Integer(1), true, node.getIdentifier(), true, node.getIdentifier()));
			delays.get();
			t.commit();
		} else {
			Transaction t = transaction();
			Delays delays = new Delays();
			node.setParent(lastAddition);
			delays.add(t.put(node));
			delays.add(t.commute(lastAddition, "addChild", node.getIdentifier()));
			delays.add(t.commute(getIdentifier(), "update", new Integer(1), false, null, true, node.getIdentifier()));
			delays.get();
			t.commit();
		}
		return node.getIdentifier();
	}

	public Identifier append(T t) {
		return addNode(new Node<T>(t));
	}

	@Override
	public void add(T t) {
		append(t);
	}

	@Override
	public void addElement(DCollection.DElement<T> element) {
		if (element instanceof Node) {
			addNode((Node<T>) element);
		} else {
			throw new RuntimeException("" + this + " can only addElement with DTree.Node");
		}
	}

	@Override
	public void each(DCollection.DEach<T> each) {
		inject(null, each);
	}

	@Override
	public <V> V inject(V v, DInject<V, T> inject) {
		if (root == null) {
			return v;
		} else {
			Delay<V> d = envoy(new InjectEnvoy<V, T>(v, inject), root);
			return d.get();
		}
	}

}