package cx.ath.troja.chordless.event;

import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.nja.Identifier;

public class DeleteEvent {

	private final Identifier key;

	private final Entry entry;

	public DeleteEvent(final Identifier key, final Entry entry) {
		this.key = key;
		this.entry = entry;
	}

	public Identifier getKey() {
		return key;
	}

	public Entry getEntry() {
		return entry;
	}
}
