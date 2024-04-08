package tech.powerjob.server.core.workflow.complement.event;

import com.google.common.base.Preconditions;

public class Event {

	private final Object runner;
	private final EventType type;
	private final EventData eventData;
	private final long time;

	private Event(final Object runner, final EventType type, final EventData eventData) {
		this.runner = runner;
		this.type = type;
		this.eventData = eventData;
		this.time = System.currentTimeMillis();
	}

	/**
	 * Creates a new event.
	 *
	 * @param runner    runner.
	 * @param type      type.
	 * @param eventData EventData, null is not allowed.
	 * @return New Event instance.
	 * @throws NullPointerException if EventData is null.
	 */
	public static Event create(final Object runner, final EventType type, final EventData eventData) throws NullPointerException {
		Preconditions.checkNotNull(eventData, "EventData was null");
		return new Event(runner, type, eventData);
	}

	public Object getRunner() {
		return this.runner;
	}

	public EventType getType() {
		return this.type;
	}

	public long getTime() {
		return this.time;
	}

	public EventData getData() {
		return this.eventData;
	}

}
