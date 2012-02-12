package cx.ath.troja.nja;

import java.util.AbstractSequentialList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AtomicLinkedList<T> extends AbstractSequentialList<T> {

	public interface Matcher<T> {
		public boolean matches(T t1, T t2);
	}

	public static class MatchResult<T> {
		public T result;

		public boolean matches;

		public MatchResult(T result, boolean matches) {
			this.result = result;
			this.matches = matches;
		}
	}

	public static class OrderException extends RuntimeException {
		public OrderException(AtomicLinkedList<?> list, Object object, State<?> state) {
			super("" + object + " should not be before " + state + " in " + list);
		}

		public OrderException(AtomicLinkedList<?> list, State<?> state, Object object) {
			super("" + object + " should not be after " + state + " in " + list);
		}
	}

	public static class AtomicLinkedListIterator<T> implements ListIterator<T> {
		private State<T> nextState;

		private State<T> currentState;

		private State<T> lastState;

		private AtomicLinkedList<T> list;

		private AtomicInteger nextIndex;

		public AtomicLinkedListIterator(AtomicLinkedList<T> list) {
			this.list = list;
			this.nextIndex = new AtomicInteger(0);
			this.nextState = list.getHeadState();
			this.currentState = null;
			this.lastState = null;
		}

		@Override
		public int previousIndex() {
			if (currentState == null) {
				throw new RuntimeException("" + this + " has not yet had a next()");
			} else {
				return list.indexOf(currentState);
			}
		}

		public MatchResult<T> find(Matcher<T> matcher, T t) {
			while (hasNext()) {
				T next = next();
				if (matcher.matches(next, t)) {
					return new MatchResult<T>(next, true);
				}
				if (list.getComparator() != null && list.getComparator().compare(next, t) > 0) {
					return new MatchResult<T>(next, false);
				}
			}
			return new MatchResult<T>(null, false);
		}

		@Override
		public T previous() {
			throw new RuntimeException("Not implemented");
		}

		public State<T> nextState() {
			return nextState;
		}

		public State<T> currentState() {
			return currentState;
		}

		@Override
		public int nextIndex() {
			return list.indexOf(nextState);
		}

		@Override
		public boolean hasNext() {
			return nextState.getEntry() != null;
		}

		@Override
		public boolean hasPrevious() {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public T next() {
			if (hasNext()) {
				lastState = currentState;
				currentState = nextState;
				nextState = nextState.next();
				return currentState.getEntry().getData();
			} else {
				throw new RuntimeException("" + this + " has no next()");
			}
		}

		@Override
		public void remove() {
			if (currentState == null) {
				throw new RuntimeException("" + this + " has not yet had a next()");
			} else {
				currentState.getEntry().setState(currentState, new State<T>(currentState.getEntry(), currentState.getNext(), true));
				currentState = currentState.getEntry().getState();
				if (currentState.isRemoved()) {
					nextState = currentState.next();
					if (lastState == null) {
						list.setHead(currentState.getEntry(), nextState.getEntry());
					} else {
						lastState.getEntry().setState(lastState, new State<T>(lastState.getEntry(), nextState.getEntry(), lastState.isRemoved()));
					}
				} else {
					remove();
				}
			}
		}

		private void validateOrder(State<T> s, T t) {
			if (list.getComparator() != null && s != null && s.getEntry() != null && list.getComparator().compare(s.getEntry().getData(), t) > 0) {
				throw new OrderException(list, s, t);
			}
		}

		private void validateOrder(T t, State<T> s) {
			if (list.getComparator() != null && s != null && s.getEntry() != null && list.getComparator().compare(s.getEntry().getData(), t) < 0) {
				throw new OrderException(list, t, s);
			}
		}

		@Override
		public void set(T t) {
			if (currentState == null) {
				throw new RuntimeException("" + this + " has not yet had a next()");
			} else {
				validateOrder(lastState, t);
				validateOrder(t, nextState);
				currentState.getEntry().setData(t);
			}
		}

		/**
		 * After an append the next entry will not be changed.
		 * 
		 * @param t
		 *            an element to prepend before the last returned entry and after the one returned before that
		 * @param ordered
		 *            if false the method will retry in case of failure, otherwise return false
		 * @return false if an ordered prepend failed
		 */
		public boolean prepend(T t, boolean ordered) {
			validateOrder(lastState, t);
			validateOrder(t, currentState);
			Entry<T> newEntry = new Entry<T>(t);
			if (currentState == null) {
				newEntry.setState(null, new State<T>(newEntry, nextState.getEntry(), false));
				if (list.setHead(nextState.getEntry(), newEntry)) {
					currentState = newEntry.getState();
					nextState = currentState.next();
					return true;
				} else {
					nextState = list.getHeadState();
					if (ordered) {
						return false;
					} else {
						return prepend(t, ordered);
					}
				}
			} else {
				newEntry.setState(null, new State<T>(newEntry, currentState.getEntry(), false));
				if (lastState == null) {
					if (list.setHead(currentState.getEntry(), newEntry)) {
						currentState = newEntry.getState();
						nextState = currentState.next();
						return true;
					} else {
						currentState = list.getHeadState();
						nextState = currentState.next();
						if (ordered) {
							return false;
						} else {
							return prepend(t, ordered);
						}
					}
				} else {
					if (lastState.getEntry().setState(lastState, new State<T>(lastState.getEntry(), newEntry, lastState.isRemoved()))) {
						lastState = lastState.getEntry().getState();
						currentState = lastState.next();
						nextState = currentState.next();
						return true;
					} else {
						lastState = lastState.getEntry().getState();
						currentState = lastState.next();
						nextState = currentState.next();
						if (ordered) {
							return false;
						} else {
							return prepend(t, ordered);
						}
					}
				}
			}
		}

		/**
		 * After an append the next entry will be the one appended.
		 * 
		 * @param t
		 *            an element to append after the last returned entry but before the next one
		 * @param ordered
		 *            if false the method will retry in case of failure, otherwise return false
		 * @return false if an ordered append failed
		 */
		public boolean append(T t, boolean ordered) {
			validateOrder(currentState, t);
			validateOrder(t, nextState);
			Entry<T> newEntry = new Entry<T>(t);
			newEntry.setState(null, new State<T>(newEntry, nextState.getEntry(), false));
			if (currentState == null) {
				if (list.setHead(nextState.getEntry(), newEntry)) {
					nextState = list.getHeadState();
					return true;
				} else {
					nextState = list.getHeadState();
					if (ordered) {
						return false;
					} else {
						return append(t, ordered);
					}
				}
			} else {
				if (currentState.getEntry().setState(currentState, new State<T>(currentState.getEntry(), newEntry, currentState.isRemoved()))) {
					currentState = currentState.getEntry().getState();
					nextState = currentState.next();
					return true;
				} else {
					currentState = currentState.getEntry().getState();
					nextState = currentState.next();
					if (ordered) {
						return false;
					} else {
						return append(t, ordered);
					}
				}
			}
		}

		@Override
		public void add(T t) {
			append(t, false);
		}
	}

	private static class State<T> {
		private Entry<T> entry;

		private Entry<T> next;

		private boolean removed;

		public State(Entry<T> entry, Entry<T> next, boolean removed) {
			this.entry = entry;
			this.next = next;
			this.removed = removed;
		}

		public String toString() {
			return "" + entry + " => " + next;
		}

		public Entry<T> getEntry() {
			return entry;
		}

		public State<T> next() {
			if (next == null) {
				return new State<T>(null, null, false);
			} else {
				State<T> nextState = next.getState();
				if (nextState.isRemoved()) {
					entry.setState(this, new State<T>(entry, nextState.getNext(), removed));
					return entry.getState().next();
				} else {
					return nextState;
				}
			}
		}

		public Entry<T> getNext() {
			return next;
		}

		public boolean isRemoved() {
			return removed;
		}
	}

	private static class Entry<T> {
		private AtomicReference<State<T>> state = new AtomicReference<State<T>>();

		private T data;

		public Entry(T data) {
			this.data = data;
		}

		public State<T> getState() {
			return state.get();
		}

		public boolean setState(State<T> o, State<T> n) {
			return state.compareAndSet(o, n);
		}

		public String toString() {
			return data + (getState().isRemoved() ? " (removed)" : "");
		}

		public T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}
	}

	private AtomicReference<Entry<T>> head = new AtomicReference<Entry<T>>();

	private Comparator<T> comparator = null;

	public AtomicLinkedList() {
		this(null, null);
	}

	private Comparator<T> getComparator() {
		return comparator;
	}

	public AtomicLinkedList(AtomicLinkedListIterator<T> iterator, Comparator<T> comparator) {
		if (iterator != null) {
			if (iterator.currentState() != null) {
				this.head.set(iterator.currentState().getEntry());
			} else {
				this.head.set(iterator.nextState().getEntry());
			}
		}
		this.comparator = comparator;
	}

	@Override
	public boolean add(T t) {
		if (comparator == null) {
			return super.add(t);
		} else {
			AtomicLinkedListIterator<T> iterator = (AtomicLinkedListIterator<T>) listIterator();
			while (iterator.hasNext()) {
				T next = iterator.next();
				if (comparator.compare(t, next) < 0) {
					if (iterator.prepend(t, true)) {
						return true;
					} else {
						return add(t);
					}
				}
			}
			if (iterator.append(t, true)) {
				return true;
			} else {
				return add(t);
			}
		}
	}

	public String toLongString() {
		StringBuffer returnValue = new StringBuffer();
		State<T> state = getHeadState();
		while (state != null) {
			returnValue.append("" + state + "\n");
			if (state.getNext() != null) {
				state = state.getNext().getState();
			} else {
				state = null;
			}
		}
		return returnValue.toString();
	}

	public State<T> getHeadState() {
		Entry<T> entry = head.get();
		if (entry == null) {
			return new State<T>(null, null, false);
		} else {
			State<T> state = entry.getState();
			if (state.isRemoved()) {
				setHead(entry, state.next().getEntry());
				return getHeadState();
			} else {
				return state;
			}
		}
	}

	private int indexOf(State<T> state) {
		AtomicLinkedListIterator<T> iterator = (AtomicLinkedListIterator<T>) iterator();
		int returnValue = 0;
		while (iterator.hasNext()) {
			if (iterator.nextState() == state) {
				return returnValue;
			}
			iterator.next();
			returnValue++;
		}
		return -1;
	}

	public void validate() {
		AtomicLinkedListIterator<T> iterator = (AtomicLinkedListIterator<T>) listIterator();
		while (iterator.hasNext()) {
			if (iterator.nextState().isRemoved()) {
				throw new RuntimeException("" + this + " is invalid: " + iterator.nextState() + " is removed");
			}
			T next = iterator.next();
			if (comparator != null && iterator.hasNext() && comparator.compare(next, iterator.nextState().getEntry().getData()) > 0) {

				throw new RuntimeException("" + this + " is invalid: " + next + " < " + iterator.nextState().getEntry().getData());
			}
		}
	}

	public boolean setHead(Entry<T> o, Entry<T> n) {
		return head.compareAndSet(o, n);
	}

	public int size() {
		Iterator<T> iterator = iterator();
		int returnValue = 0;
		while (iterator.hasNext()) {
			iterator.next();
			returnValue++;
		}
		return returnValue;
	}

	public ListIterator<T> listIterator(int index) {
		return new AtomicLinkedListIterator<T>(this);
	}

}