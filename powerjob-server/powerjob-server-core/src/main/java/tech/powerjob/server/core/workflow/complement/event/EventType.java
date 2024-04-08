package tech.powerjob.server.core.workflow.complement.event;

/**
 * Enum class defining the list of supported event types.
 */
public enum EventType {
  COMPLEMENT_STARTED,
  COMPLEMENT_FINISHED,
  FLOW_STARTED,
  FLOW_FINISHED,
  JOB_STARTED,
  JOB_FINISHED,
  JOB_STATUS_CHANGED
}
