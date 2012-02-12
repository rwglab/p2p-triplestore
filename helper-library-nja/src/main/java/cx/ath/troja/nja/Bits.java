package cx.ath.troja.nja;

import java.io.Serializable;
import java.util.SortedSet;
import java.util.TreeSet;

public class Bits implements Serializable {

	public static class Sequence implements Serializable, Comparable<Sequence> {
		public long fromInclusive;

		public long toExclusive;

		public Sequence(long fromInclusive, long toExclusive) {
			if (fromInclusive > toExclusive) {
				throw new IllegalArgumentException("First argument must be less than or equal to second argument");
			}
			this.fromInclusive = fromInclusive;
			this.toExclusive = toExclusive;
		}

		public Sequence(Sequence s) {
			fromInclusive = s.fromInclusive;
			toExclusive = s.toExclusive;
		}

		public long length() {
			return toExclusive - fromInclusive;
		}

		public boolean within(long l) {
			return l >= fromInclusive && l < toExclusive;
		}

		public boolean within(Sequence s) {
			return s.fromInclusive >= fromInclusive && s.toExclusive <= toExclusive;
		}

		public boolean overlaps(Sequence s) {
			return within(s.fromInclusive) || within(s.toExclusive - 1);
		}

		public boolean equals(Object o) {
			if (o instanceof Sequence) {
				Sequence other = (Sequence) o;
				return fromInclusive == other.fromInclusive && toExclusive == other.toExclusive;
			} else {
				return false;
			}
		}

		public String toString() {
			if (toExclusive == Long.MAX_VALUE) {
				return "" + fromInclusive + "-MAX_VALUE";
			} else {
				return "" + fromInclusive + "-" + (toExclusive - 1);
			}
		}

		public int compareTo(Sequence other) {
			return new Long(fromInclusive).compareTo(new Long(other.fromInclusive));
		}
	}

	private TreeSet<Sequence> sequences = new TreeSet<Sequence>();

	/**
	 * Used for testing which is why there is no intelligence in the adding of sequences here.
	 * 
	 * Ie do NOT use #set to add these ranges, since we test the #set method against examples created with this method.
	 */
	public Bits(long... ranges) {
		for (int i = 0; i < ranges.length; i += 2) {
			add(new Sequence(ranges[i], ranges[i + 1]));
		}
	}

	public Bits() {
	}

	public Bits(Bits b) {
		for (Sequence sequence : b.sequences) {
			add(new Sequence(sequence));
		}
	}

	public SortedSet<Sequence> overlapping(long fromInclusive, long toExclusive) {
		SortedSet<Sequence> within = new TreeSet<Sequence>(sequences.subSet(new Sequence(fromInclusive, fromInclusive), new Sequence(toExclusive,
				toExclusive)));
		Sequence closestBelow = sequences.floor(new Sequence(fromInclusive, fromInclusive));
		if (closestBelow != null && fromInclusive < closestBelow.toExclusive) {
			within.add(closestBelow);
		}
		return within;
	}

	private void add(Sequence sequence) {
		if (sequence.length() > 0) {
			Sequence above = sequences.ceiling(new Sequence(sequence.toExclusive, sequence.toExclusive));
			if (above != null && above.fromInclusive == sequence.toExclusive) {
				sequences.remove(above);
				sequence.toExclusive = above.toExclusive;
			}
			Sequence below = sequences.floor(new Sequence(sequence));
			if (below != null && below.toExclusive == sequence.fromInclusive) {
				sequences.remove(below);
				sequence.fromInclusive = below.fromInclusive;
			}
			sequences.add(sequence);
		}
	}

	private void remove(Sequence sequence) {
		sequences.remove(sequence);
	}

	public boolean isEmpty() {
		return sequences.isEmpty();
	}

	public void andNot(Bits b) {
		for (Sequence sequence : b.sequences) {
			clear(sequence.fromInclusive, sequence.toExclusive);
		}
	}

	public long cardinality() {
		long returnValue = 0;
		for (Sequence sequence : sequences) {
			returnValue += sequence.length();
		}
		return returnValue;
	}

	public String toString() {
		StringBuffer returnValue = new StringBuffer(this.getClass().getName() + " ");
		for (Sequence sequence : sequences) {
			returnValue.append(sequence.toString()).append(" ");
		}
		return returnValue.toString();
	}

	private void assertLess(long a, long b) {
		if (a >= b) {
			throw new IllegalArgumentException("First argument must be less than second");
		}
	}

	public void flip() {
		flip(0, Long.MAX_VALUE);
	}

	public void flip(long fromInclusive, long toExclusive) {
		Sequence below = sequences.floor(new Sequence(fromInclusive, fromInclusive));
		if (below != null && below.toExclusive > fromInclusive) {
			remove(below);
			add(new Sequence(below.fromInclusive, fromInclusive));
			fromInclusive = below.toExclusive;
		}
		for (Sequence sequence : new TreeSet<Sequence>(sequences.subSet(new Sequence(fromInclusive, fromInclusive), new Sequence(toExclusive,
				toExclusive)))) {
			if (sequence.fromInclusive >= fromInclusive) {
				remove(sequence);
				add(new Sequence(fromInclusive, sequence.fromInclusive));
				if (sequence.toExclusive > toExclusive) {
					add(new Sequence(toExclusive, sequence.toExclusive));
				}
			}
			fromInclusive = sequence.toExclusive;
		}
		if (fromInclusive < toExclusive) {
			add(new Sequence(fromInclusive, toExclusive));
		}
	}

	public TreeSet<Sequence> getSequences() {
		return sequences;
	}

	public Bits get(long fromInclusive, long toExclusive) {
		assertLess(fromInclusive, toExclusive);
		Bits returnValue = new Bits();
		for (Sequence sequence : overlapping(fromInclusive, toExclusive)) {
			returnValue.set(Math.max(fromInclusive, sequence.fromInclusive), Math.min(toExclusive, sequence.toExclusive));
		}
		return returnValue;
	}

	public boolean get(long index) {
		return !overlapping(index, index + 1).isEmpty();
	}

	public void clear(long fromInclusive, long toExclusive) {
		assertLess(fromInclusive, toExclusive);
		Sequence clearSequence = new Sequence(fromInclusive, toExclusive);
		for (Sequence sequence : overlapping(fromInclusive, toExclusive)) {
			if (clearSequence.within(sequence)) {
				sequences.remove(sequence);
			} else if (sequence.fromInclusive <= fromInclusive && sequence.toExclusive <= toExclusive) {
				sequence.toExclusive = fromInclusive;
			} else if (sequence.fromInclusive >= fromInclusive && sequence.toExclusive >= toExclusive) {
				remove(sequence);
				sequence.fromInclusive = toExclusive;
				add(sequence);
			} else if (sequence.fromInclusive <= fromInclusive && sequence.toExclusive >= toExclusive) {
				add(new Sequence(toExclusive, sequence.toExclusive));
				sequence.toExclusive = fromInclusive;
			} else {
				throw new RuntimeException("" + sequence + " was expected to be overlapping " + clearSequence);
			}
		}
	}

	public void clear(long index) {
		clear(index, index + 1);
	}

	public long logicalSize() {
		if (sequences.size() == 0) {
			return 0;
		} else {
			return sequences.last().toExclusive;
		}
	}

	public void set(long fromInclusive, long toExclusive) {
		assertLess(fromInclusive, toExclusive);
		for (Sequence sequence : overlapping(fromInclusive, toExclusive)) {
			remove(sequence);
			fromInclusive = Math.min(sequence.fromInclusive, fromInclusive);
			toExclusive = Math.max(sequence.toExclusive, toExclusive);
		}
		add(new Sequence(fromInclusive, toExclusive));
	}

	public void set(long index) {
		set(index, index + 1);
	}

}