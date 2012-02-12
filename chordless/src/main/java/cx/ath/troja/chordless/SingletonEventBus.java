package cx.ath.troja.chordless;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import java.util.concurrent.Executors;

public class SingletonEventBus {
	
	private static EventBus eventBus;

	public static EventBus getEventBus() {
		if (eventBus == null) {
			eventBus = new AsyncEventBus(Executors.newCachedThreadPool());
		}
		return eventBus;
	}
}
