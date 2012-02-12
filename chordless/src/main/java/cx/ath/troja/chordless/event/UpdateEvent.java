package cx.ath.troja.chordless.event;

import cx.ath.troja.nja.Identifier;

public class UpdateEvent {

	private final Identifier key;

	private final Object valueRemoved;

	private final Object valueAdded;

	public UpdateEvent(final Identifier key, final Object valueRemoved, final Object valueAdded) {
		this.key = key;
		this.valueRemoved = valueRemoved;
		this.valueAdded = valueAdded;
	}

	public Identifier getKey() {
		return key;
	}

	public Object getValueAdded() {
		return valueAdded;
	}

	public Object getValueRemoved() {
		return valueRemoved;
	}
}
