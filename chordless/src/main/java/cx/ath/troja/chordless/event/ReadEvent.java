package cx.ath.troja.chordless.event;

import cx.ath.troja.chordless.dhash.Entry;
import cx.ath.troja.nja.Identifier;

public class ReadEvent {

	private final Identifier key;

	private final Entry value;

	public ReadEvent(final Identifier key, final Entry value) {
		this.key = key;
		this.value = value;
	}

	public Identifier getKey() {
		return key;
	}

	public Entry getValue() {
		return value;
	}
}
