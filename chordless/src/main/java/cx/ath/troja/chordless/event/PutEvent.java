package cx.ath.troja.chordless.event;

import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.dhash.Entry;

public class PutEvent {

	private final Entry entry;

	public PutEvent(final Entry entry) {
		this.entry = entry;
	}

	public Entry getEntry() {
		return entry;
	}
}
