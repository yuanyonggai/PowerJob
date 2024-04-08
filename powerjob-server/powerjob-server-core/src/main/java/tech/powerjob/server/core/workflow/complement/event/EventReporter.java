package tech.powerjob.server.core.workflow.complement.event;

import java.util.Map;

/**
 * Implement this interface to report flow and task events
 */
public interface EventReporter {

  boolean report(EventType eventType, Map<String, String> metadata);
}
