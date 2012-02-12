/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.structures;

import java.io.Serializable;

import cx.ath.troja.chordless.dhash.Delay;
import cx.ath.troja.chordless.dhash.Delays;
import cx.ath.troja.chordless.dhash.Persistent;
import cx.ath.troja.chordless.dhash.Persister;
import cx.ath.troja.chordless.dhash.transactions.Transaction;
import cx.ath.troja.nja.Identifier;

public class DList<T> extends Persistent implements DCollection.Primitive<T> {

	private static final long serialVersionUID = 1L;

	public static class DListIterator<T> implements DCollection.PrimitiveDIterator<T> {
		private Identifier previous;

		private Identifier location;

		private Identifier parent;

		private Identifier last;

		public DListIterator(Persister p, Identifier par, Identifier i) {
			location = i;
			if (location != null) {
				Delay<ElementData<T>> returnValue = p.exec(location, "getData");
				previous = returnValue.get().previous;
			}
			parent = par;
			last = null;
		}

		@Override
		public Identifier getLast() {
			return last;
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
		public T next(Persister p) {
			if (location == null) {
				throw new RuntimeException("You can not call next on a DListIterator that doesn't have a next");
			} else {
				Delay<ElementData<T>> returnValue = p.exec(location, "getData");
				last = location;
				previous = location;
				location = returnValue.get().next;
				return returnValue.get().value;
			}
		}

		@Override
		public T previous(Persister p) {
			if (previous == null) {
				throw new RuntimeException("You can not call previous on a DListIterator that doesn't have a previous");
			} else {
				Delay<ElementData<T>> returnValue = p.exec(previous, "getData");
				last = previous;
				location = previous;
				previous = returnValue.get().previous;
				return returnValue.get().value;
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
						"You can not remove from a DListIterator before you have fetched at least one entry from it since creation or last remove");
			} else {
				Delay<ElementData<T>> delay = p.exec(last, "getData");
				previous = delay.get().previous;
				location = delay.get().next;
				p.exec(last, "remove").get();
				last = null;
			}
		}

		@Override
		public void add(Persister p, T t) {
			if (location == null) {
				Delay<Identifier> delay = p.exec(parent, "append", t);
				location = delay.get();
			} else {
				p.exec(location, "append", t).get();
			}
		}
	}

	public static abstract class Inject<V, T> extends DCollection.DefaultDInject<V, T> {
		@Override
		public Element<T> getElement() {
			return (Element<T>) super.getElement();
		}

		public Delay<? extends Object> appendAsync(T t) {
			return getElement().appendAsync(t);
		}

		public Delay<? extends Object> prependAsync(T t) {
			return getElement().prependAsync(t);
		}
	}

	public static abstract class Each<T> extends Inject<Object, T> implements DCollection.DEach<T> {
		@Override
		public Object inject(Object o, T t) {
			each(t);
			return null;
		}
	}

	private static class FindElementEnvoy<T> extends Persister.Envoy<Element<T>> {
		private T target;

		public FindElementEnvoy(T t) {
			target = t;
		}

		public void handle(Element<T> element) {
			if (element.getValue().equals(target)) {
				returnHome(element);
			} else if (element.getNext() == null) {
				returnHome(null);
			} else {
				redirect(element.getNext());
			}
		}
	}

	private static class RemoveElementEnvoy<T> extends Persister.Envoy<Element<T>> {
		private T target;

		public RemoveElementEnvoy(T t) {
			target = t;
		}

		public void handle(Element<T> element) {
			Identifier next = element.next;
			if (element.getValue().equals(target)) {
				element.remove();
				returnHome(null);
			} else if (next == null) {
				returnHome(null);
			} else {
				redirect(next);
			}
		}
	}

	private static class GetIndexEnvoy<T> extends Persister.Envoy<Element<T>> {
		private long ttl;

		public GetIndexEnvoy(long i) {
			ttl = i;
		}

		public void handle(Element<T> element) {
			if (ttl != 0) {
				if (ttl > 0) {
					ttl--;
					if (element.getNext() == null) {
						throw new IndexOutOfBoundsException("The list ends, while my ttl is " + ttl);
					} else {
						redirect(element.getNext());
					}
				} else {
					ttl++;
					if (element.getPrevious() == null) {
						throw new IndexOutOfBoundsException("The list ends, while my ttl is " + ttl);
					} else {
						redirect(element.getPrevious());
					}
				}
			} else {
				returnHome(element);
			}
		}
	}

	private static class InjectEnvoy<T, V> extends Persister.Envoy<Element<T>> {
		private DInject<V, T> inject;

		private V v;

		public InjectEnvoy(V v, DInject<V, T> i) {
			inject = i;
			this.v = v;
		}

		public void handle(Element<T> element) {
			Identifier next = element.next;
			v = inject.perform(v, element, element);
			if (next == null) {
				returnHome(v);
			} else {
				redirect(next);
			}
		}
	}

	private static class VerifyInject<T> extends Inject<Integer, T> {
		private Identifier lastElement = null;

		public String toString() {
			return "<" + this.getClass() + " lastElement=" + lastElement + ">";
		}

		public Integer inject(Integer sum, T t) {
			Delay<DList> listDelay = getPersister().get(getElement().getList());
			DList list = listDelay.get();
			if (lastElement == null) {
				if (!list.getFirst().equals(getElement().getIdentifier())) {
					throw new RuntimeException("First element (" + getElement() + ") is not first according to DList (" + list + ")!");
				}
				if (getElement().previous != null) {
					throw new RuntimeException("First element has a previous element!");
				}
			} else {
				Delay<DList.Element> elementDelay = getPersister().get(getElement().getPrevious());
				DList.Element previousElement = elementDelay.get();
				if (!previousElement.getNext().equals(getElement().getIdentifier())) {
					throw new RuntimeException("Previous element doesnt agree that this element is next!");
				}
				if (!lastElement.equals(getElement().getPrevious())) {
					throw new RuntimeException("Element does not recognize last element as previous!");
				}
			}
			if (getElement().getNext() == null) {
				if (!list.getLast().equals(getElement().getIdentifier())) {
					throw new RuntimeException("List does not agree that last element is last!");
				}
			}
			lastElement = getElement().getIdentifier();
			return sum + 1;
		}
	}

	private static class ClearInject<T> extends Inject<Object, T> {
		public Object inject(Object o, T t) {
			remove();
			return null;
		}
	}

	public static class ElementData<V> implements Serializable {
		public Identifier next;

		public Identifier previous;

		public V value;

		public ElementData(Identifier pre, V v, Identifier ne) {
			next = ne;
			previous = pre;
			value = v;
		}
	}

	public static class Element<V> extends DCollection.DElement<V> {
		private static final long serialVersionUID = 1L;

		private Identifier previous;

		private Identifier next;

		private Identifier list;

		public Element(V v) {
			value = v;
			id = Identifier.random();
			list = null;
			previous = null;
			next = null;
		}

		public void untaint() {
			setTaint(false);
		}

		public String toString() {
			return "<" + getClass().getName() + " " + previous + " " + getIdentifier() + "(" + value + ") " + next + ">";
		}

		public ElementData<V> getData() {
			return new ElementData<V>(previous, value, next);
		}

		public Identifier getNext() {
			return next;
		}

		public Identifier getPrevious() {
			return previous;
		}

		public Identifier getList() {
			return list;
		}

		public void setNext(Identifier i) {
			next = i;
		}

		public void setPrevious(Identifier i) {
			previous = i;
		}

		public void setList(Identifier i) {
			list = i;
		}

		protected <T> Element<T> getNewElement(T t) {
			return new Element<T>(t);
		}

		public void append(V v) {
			appendAsync(v).get();
		}

		public Delay<? extends Object> appendAsync(V t) {
			final Delays delays = new Delays();
			Element<V> element = getNewElement(t);
			element.setList(list);
			element.setPrevious(getIdentifier());
			element.setNext(next);
			final Transaction trans = transaction();
			if (next == null) {
				delays.add(trans.exec(list, "update", 1, false, null, true, element.getIdentifier()));
			} else {
				delays.add(trans.exec(next, "setPrevious", element.getIdentifier()));
				delays.add(trans.exec(list, "update", 1, false, null, false, null));
			}
			delays.add(trans.put(element));
			delays.add(trans.exec(getIdentifier(), "setNext", element.getIdentifier()));
			setTaint(false);
			return new Delay<Object>() {
				public Object get(long timeout) {
					return get();
				}

				public Object get() {
					delays.get();
					trans.commit();
					return null;
				}
			};
		}

		public void prepend(V v) {
			prependAsync(v).get();
		}

		public Delay<? extends Object> prependAsync(V t) {
			final Delays delays = new Delays();
			Element<V> element = getNewElement(t);
			element.setList(list);
			element.setNext(getIdentifier());
			element.setPrevious(previous);
			final Transaction trans = transaction();
			if (previous == null) {
				delays.add(trans.exec(list, "update", 1, true, element.getIdentifier(), false, null));
			} else {
				delays.add(trans.exec(previous, "setNext", element.getIdentifier()));
				delays.add(trans.exec(list, "update", 1, false, null, false, null));
			}
			delays.add(trans.put(element));
			delays.add(trans.exec(getIdentifier(), "setPrevious", element.getIdentifier()));
			setTaint(false);
			return new Delay<Object>() {
				public Object get(long timeout) {
					return get();
				}

				public Object get() {
					delays.get();
					trans.commit();
					return null;
				}
			};
		}

		@Override
		public Delay<? extends Object> removeAsync() {
			final Delays delays = new Delays();
			boolean useNewFirst = false;
			boolean useNewLast = false;
			final Transaction t = transaction();
			if (previous == null) {
				useNewFirst = true;
			} else {
				delays.add(t.exec(previous, "setNext", next));
			}
			if (next == null) {
				useNewLast = true;
			} else {
				delays.add(t.exec(next, "setPrevious", previous));
			}
			delays.add(t.exec(list, "update", -1, useNewFirst, next, useNewLast, previous));
			delays.add(t.del(getIdentifier(), null));
			setTaint(false);
			return new Delay<Object>() {
				public Object get(long timeout) {
					return get();
				}

				public Object get() {
					delays.get();
					t.commit();
					return null;
				}
			};
		}
	}

	private Identifier first;

	private Identifier last;

	private long size;

	private Identifier id;

	public DList() {
		id = Identifier.random();
		first = null;
		last = null;
		size = 0;
	}

	public void setId(Identifier x) {
		id = x;
	}

	public String toString() {
		return "<" + getClass().getName() + " id=" + id + " first=" + first + " last=" + last + " size=" + size + ">";
	}

	public Object getId() {
		return id;
	}

	public Identifier getFirst() {
		return first;
	}

	public Identifier getLast() {
		return last;
	}

	public void update(Integer sizediff, Boolean useNewFirst, Identifier newFirst, Boolean useNewLast, Identifier newLast) {
		size += sizediff;
		if (useNewFirst) {
			first = newFirst;
		}
		if (useNewLast) {
			last = newLast;
		}
	}

	@Override
	public void each(DCollection.DEach<T> each) {
		inject(null, each);
	}

	@Override
	public <V> V inject(V v, DCollection.DInject<V, T> inject) {
		setTaint(false);
		if (first == null) {
			return v;
		} else {
			Delay<V> delay = envoy(new InjectEnvoy<T, V>(v, inject), first);
			return delay.get();
		}
	}

	public void clear() {
		inject(null, new ClearInject<T>());
		setTaint(false);
	}

	public Element<T> elementAt(long l) {
		setTaint(false);
		Delay<Element<T>> delay = null;
		if (l >= 0) {
			if (size > l) {
				delay = envoy(new GetIndexEnvoy<T>(l), first);
			} else {
				throw new IndexOutOfBoundsException("" + this + " is only " + size + " long, unable to get index " + l);
			}
		} else {
			l++;
			if (size > Math.abs(l)) {
				delay = envoy(new GetIndexEnvoy<T>(l), last);
			} else {
				throw new IndexOutOfBoundsException("" + this + " is only " + size + " long, unable to get index " + l);
			}
		}
		return delay.get();
	}

	@Override
	public boolean contains(T t) {
		setTaint(false);
		if (size == 0) {
			return false;
		} else {
			Delay<Element<T>> delay = envoy(new FindElementEnvoy<T>(t), first);
			return delay.get() != null;
		}
	}

	@Override
	public void remove(T t) {
		setTaint(false);
		if (size > 0) {
			envoy(new RemoveElementEnvoy<T>(t), first).get();
		}
	}

	@Override
	protected Delay<? extends Object> removeAsync() {
		clear();
		return super.removeAsync();
	}

	public void verify() {
		if (inject(new Integer(0), new VerifyInject<T>()).intValue() != (int) size) {
			throw new RuntimeException("Verified size differs from real size!");
		}
	}

	public Identifier appendElement(final Element<T> element) {
		setTaint(false);
		if (size == 0) {
			return addFirst(element);
		} else {
			element.setPrevious(last);
			element.setNext(null);
			element.setList(getIdentifier());
			Delays delays = new Delays();
			Transaction t = transaction();
			delays.add(t.put(element));
			delays.add(t.exec(last, "setNext", element.getIdentifier()));
			delays.add(t.exec(getIdentifier(), "update", new Integer(1), false, null, true, element.getIdentifier()));
			delays.get();
			t.commit();
			return element.getIdentifier();
		}
	}

	public Identifier prependElement(final Element<T> element) {
		setTaint(false);
		if (size == 0) {
			return addFirst(element);
		} else {
			element.setPrevious(null);
			element.setNext(first);
			element.setList(getIdentifier());
			Delays delays = new Delays();
			Transaction t = transaction();
			delays.add(t.put(element));
			delays.add(t.exec(first, "setPrevious", element.getIdentifier()));
			delays.add(t.exec(getIdentifier(), "update", new Integer(1), true, element.getIdentifier(), false, null));
			delays.get();
			t.commit();
			return element.getIdentifier();
		}
	}

	private Identifier addFirst(final Element<T> element) {
		element.setPrevious(null);
		element.setNext(null);
		element.setList(getIdentifier());
		Transaction t = transaction();
		Delays delays = new Delays();
		delays.add(t.put(element));
		delays.add(t.exec(getIdentifier(), "update", new Integer(1), true, element.getIdentifier(), true, element.getIdentifier()));
		delays.get();
		t.commit();
		return element.getIdentifier();
	}

	@Override
	public void add(T t) {
		append(t);
	}

	@Override
	public void addElement(DCollection.DElement<T> element) {
		if (element instanceof Element) {
			appendElement((Element<T>) element);
		} else {
			throw new RuntimeException("" + this + " can only addElement with DList.Elements");
		}
	}

	@Override
	public DListIterator<T> iterator(Identifier location) {
		return new DListIterator<T>(this, getIdentifier(), location);
	}

	@Override
	public DListIterator<T> iterator() {
		return new DListIterator<T>(this, getIdentifier(), first);
	}

	protected <T> Element<T> getNewElement(T t) {
		return new Element<T>(t);
	}

	public Identifier append(T t) {
		return appendElement(getNewElement(t));
	}

	public Identifier prepend(T t) {
		return prependElement(getNewElement(t));
	}

	public T first() {
		setTaint(false);
		if (first == null) {
			throw new RuntimeException("List is empty!");
		} else {
			Delay<T> delay = exec(first, "getValue");
			return delay.get();
		}
	}

	public T last() {
		setTaint(false);
		if (first == null) {
			throw new RuntimeException("List is empty!");
		} else {
			Delay<T> delay = exec(last, "getValue");
			return delay.get();
		}
	}

	public T removeFirst() {
		setTaint(false);
		if (first == null) {
			throw new RuntimeException("List is empty!");
		} else {
			Delay<T> delay = exec(first, "getValue");
			delay.get();
			exec(first, "remove").get();
			return delay.get();
		}
	}

	public T removeLast() {
		setTaint(false);
		if (first == null) {
			throw new RuntimeException("List is empty!");
		} else {
			Delay<T> delay = exec(last, "getValue");
			delay.get();
			exec(last, "remove").get();
			return delay.get();
		}
	}

	@Override
	public long size() {
		setTaint(false);
		return size;
	}

}