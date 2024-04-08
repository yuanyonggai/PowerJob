package tech.powerjob.server.core.workflow.complement.event;

import java.util.ArrayList;
import java.util.HashSet;

public class EventHandler {

	private final HashSet<EventListener> listeners = new HashSet<>();

	public EventHandler() {
	}

	public void addListener(final EventListener listener) {
		this.listeners.add(listener);
	}

	public void fireEventListeners(final Event event) {
		final ArrayList<EventListener> listeners = new ArrayList<>(this.listeners);
		for (final EventListener listener : listeners) {
			listener.handleEvent(event);
		}
	}

	public void removeListener(final EventListener listener) {
		this.listeners.remove(listener);
	}
}
