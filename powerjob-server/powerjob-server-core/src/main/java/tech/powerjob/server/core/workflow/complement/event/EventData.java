package tech.powerjob.server.core.workflow.complement.event;

import java.util.Map;

/**
 * Carries an immutable snapshot of the status data, suitable for asynchronous message passing.
 */
public class EventData {

//  private final Status status;
//  private final String nestedId;

    /**
     * Creates a new EventData instance.
     *
     * @param status node status.
     * @param nestedId node id, corresponds to {@link ExecutableNode#getNestedId()}.
     */
//  public EventData(final Status status, final String nestedId) {
//    this.status = status;
//    this.nestedId = nestedId;
//  }

    /**
     * Creates a new EventData instance.
     */
    public EventData(final Map data) {
        //this(node.getStatus(), node.getNestedId());
//    this.nestedId = nestedId;
    }
//
//  public Status getStatus() {
//    return this.status;
//  }
//
//  public String getNestedId() {
//    return this.nestedId;
//  }

}
