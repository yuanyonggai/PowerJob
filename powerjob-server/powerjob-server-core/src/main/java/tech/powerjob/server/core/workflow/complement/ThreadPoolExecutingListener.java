package tech.powerjob.server.core.workflow.complement;

/**
 * Interface for listener to get notified before and after a task has been executed.
 */
public interface ThreadPoolExecutingListener {

	public void beforeExecute(Runnable r);

	public void afterExecute(Runnable r);
}
