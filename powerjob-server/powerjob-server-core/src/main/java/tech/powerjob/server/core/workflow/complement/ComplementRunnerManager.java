package tech.powerjob.server.core.workflow.complement;

import java.lang.Thread.State;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.server.core.service.JobService;
import tech.powerjob.server.core.workflow.WorkflowService;
import tech.powerjob.server.core.workflow.complement.event.Event;
import tech.powerjob.server.core.workflow.complement.event.EventListener;
import tech.powerjob.server.core.workflow.complement.event.EventReporter;
import tech.powerjob.server.core.workflow.complement.event.EventType;
import tech.powerjob.server.core.workflow.complement.vo.JobComplementVO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;

/**
 * ComplementRunner manager for the server side execution.
 */
@Component
@Slf4j
public class ComplementRunnerManager implements EventListener, ThreadPoolExecutingListener {

    // recently finished secs to clean up. 1 minute
    private static final int RECENTLY_FINISHED_TIME_TO_LIVE = 10 * 60 * 1000;

    private static final int DEFAULT_NUM_EXECUTING_FLOWS = 30;

    // this map is used to store the complement that have been submitted to
    // the executor service. Once a complement has been submitted, it is either
    // in the queue waiting to be executed or in executing state.
    private final Map<Future<?>, Long> submittedComplements = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, ComplementRunner> runningComplements = new ConcurrentHashMap<Long, ComplementRunner>();

    private final TrackingThreadPool executorService;

    private final CleanerThread cleanerThread;

    private final EventReporter eventReporter;

    private final int numThreads;

    private int threadPoolQueueSize = -1;

    private long lastCleanerThreadCheckTime = -1;

    final private WorkflowService workflowService;

    final private JobService jobService;

    final private WorkflowInstanceInfoRepository workflowInstanceInfoRepository;

    final private InstanceInfoRepository instanceInfoRepository;

    @Autowired
    public ComplementRunnerManager(EventReporter eventReporter,
            WorkflowService workflowService,
            JobService jobService,
            WorkflowInstanceInfoRepository workflowInstanceInfoRepository,
            InstanceInfoRepository instanceInfoRepository) {
        this.eventReporter = eventReporter;
        this.workflowService = workflowService;
        this.jobService = jobService;
        this.workflowInstanceInfoRepository = workflowInstanceInfoRepository;
        this.instanceInfoRepository = instanceInfoRepository;
        this.numThreads = DEFAULT_NUM_EXECUTING_FLOWS;
        this.executorService = createExecutorService(this.numThreads);
        this.cleanerThread = new CleanerThread();
        this.cleanerThread.start();
    }

    private TrackingThreadPool createExecutorService(final int nThreads) {
        log.info("create ExecutorService: " + nThreads);
        // the old way of using unbounded task queue.
        // if the running tasks are taking a long time or stuck, this queue
        // will be very very long.
        return new TrackingThreadPool(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setDaemon(true).build(), this);
    }

    public void submitComplement(final JobComplementVO flowComplementVO) {
        final ComplementRunner runner = createComplementRunner(flowComplementVO);
        submitComplementRunner(runner);
    }

    public boolean containsKey(final Long flowComplementKey) {
        if (this.runningComplements.containsKey(flowComplementKey)) {
            return true;
        }
        return false;
    }

    private ComplementRunner createComplementRunner(final JobComplementVO flowComplementVO) {
        ComplementRunner runner = null;
        if (flowComplementVO.isWorkflow()) {
            runner = new ComplementRunner(flowComplementVO, workflowService, null, workflowInstanceInfoRepository, null,
                    eventReporter);
        } else {
            runner = new ComplementRunner(flowComplementVO, null, jobService, null, instanceInfoRepository,
                    eventReporter);
        }

        runner.addListener(this);
        // configureFlowLevelMetrics(runner);
        return runner;
    }

    private void submitComplementRunner(final ComplementRunner runner) {
        this.runningComplements.put(runner.getFlowComplementVO().getWorkflowId(), runner);
        try {
            // The executorService already has a queue.
            // The submit method below actually returns an instance of FutureTask,
            // which implements interface RunnableFuture, which extends both
            // Runnable and Future interfaces
            final Future<?> future = this.executorService.submit(runner);
            // keep track of this future
            this.submittedComplements.put(future, runner.getFlowComplementVO().getWorkflowId());

        } catch (final RejectedExecutionException re) {
            this.runningComplements.remove(runner.getFlowComplementVO().getWorkflowId());
            final StringBuffer errorMsg = new StringBuffer(" executor can't execute any more complements ");
            if (this.executorService.isShutdown()) {
                errorMsg.append("The executor is being shut down.");
            }
            throw new PowerJobException(errorMsg.toString(), re);
        }
    }

    public void cancelComplement(final Long workflowId) {
        log.info("cancelComplement: workflowId or jobId->{} ", workflowId);
        final ComplementRunner runner = this.runningComplements.get(workflowId);
        if (runner == null) {
            // throw new ExecutorManagerException("complementation " + workflowId + " is not
            // running.");
            log.info("作业实例{}已经停止执行", workflowId);
        } else {
            this.runningComplements.remove(runner.getFlowComplementVO().getWorkflowId());
            runner.kill();
        }

    }

    @Override
    public void handleEvent(final Event event) {
        if (event.getType() == EventType.COMPLEMENT_FINISHED || event.getType() == EventType.COMPLEMENT_STARTED) {
            final ComplementRunner complementRunner = (ComplementRunner) event.getRunner();
            final JobComplementVO flowComplementVO = complementRunner.getFlowComplementVO();

            if (event.getType() == EventType.COMPLEMENT_FINISHED) {
                log.info("workflowId or jobId " + flowComplementVO.getWorkflowId() + " complement is finished.");
                this.runningComplements.remove(flowComplementVO.getWorkflowId());                
            } else if (event.getType() == EventType.COMPLEMENT_STARTED) {
                log.info("workflowId or jobId: " + flowComplementVO.getWorkflowId() + " complement is started.");                
            }
        }
    }

    public long getLastCleanerThreadCheckTime() {
        return this.lastCleanerThreadCheckTime;
    }

    public boolean isCleanerThreadActive() {
        return this.cleanerThread.isAlive();
    }

    public State getCleanerThreadState() {
        return this.cleanerThread.getState();
    }

    public boolean isExecutorThreadPoolShutdown() {
        return this.executorService.isShutdown();
    }

    public int getNumQueuedFlows() {
        return this.executorService.getQueue().size();
    }

    public int getNumRunningFlows() {
        return this.executorService.getActiveCount();
    }

    public List<Long> getRunningComplementKeys() {
        // The in progress tasks are actually of type FutureTask
        final Set<Runnable> inProgressTasks = this.executorService.getInProgressTasks();

        final List<Long> runningComplementKeys = new ArrayList<>(inProgressTasks.size());

        for (final Runnable task : inProgressTasks) {
            // add casting here to ensure it matches the expected type in
            // submittedFlows
            final Long workflowId = this.submittedComplements.get(task);
            if (workflowId != null) {
                runningComplementKeys.add(workflowId);
            } else {
                log.warn("getRunningComplementKeys: got null executionId for task: " + task);
            }
        }

        Collections.sort(runningComplementKeys);
        return runningComplementKeys;
    }

    public String getQueuedComplementKeys() {
        final List<String> complementKeysList = new ArrayList<>(this.executorService.getQueue().size());

        for (final Runnable task : this.executorService.getQueue()) {
            final Long flowComplementKeys = this.submittedComplements.get(task);
            if (flowComplementKeys != null) {
                complementKeysList.add(flowComplementKeys.toString());
            } else {
                log.warn("getQueuedFlowIds: got null executionId for queuedTask: " + task);
            }
        }
        Collections.sort(complementKeysList);
        return complementKeysList.toString();
    }

    public int getMaxNumRunningFlows() {
        return this.numThreads;
    }

    public int getTheadPoolQueueSize() {
        return this.threadPoolQueueSize;
    }

    public int getTotalNumExecutedFlows() {
        return this.executorService.getTotalTasks();
    }

    @Override
    public void beforeExecute(final Runnable r) {
    }

    @Override
    public void afterExecute(final Runnable r) {
        this.submittedComplements.remove(r);
    }

    /**
     * This shuts down the complement runner. The call is blocking and awaits
     * execution of all jobs.
     */
    @PreDestroy
    public void shutdown() {
        log.warn("Shutting down ComplementRunnerManager...");

        this.executorService.shutdown();
        boolean result = false;
        while (!result) {
            log.info("Awaiting Shutdown. # of executing flow complement: " + getNumRunningFlows());
            try {
                result = this.executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (final InterruptedException e) {
                log.error("shutdown", e);
                Thread.currentThread().interrupt();
                this.executorService.shutdownNow();
            }
        }
        log.warn("Shutdown ComplementRunnerManager complete.");
    }

    /**
     * This attempts shuts down the complement runner immediately (unsafe). This
     * doesn't wait for jobs to
     * finish but interrupts all threads.
     */
    public void shutdownNow() {
        log.warn("Shutting down ComplementRunnerManager now...");
        this.executorService.shutdownNow();
        // this.triggerManager.shutdown();
    }

    private class CleanerThread extends Thread {

        // Every 5 mins kill flows running longer than allowed max running time
        private static final long LONG_RUNNING_FLOW_KILLING_INTERVAL_MS = 5 * 60 * 1000;
        private final long complementMaxRunningTimeInMins = -1;
        private boolean shutdown = false;
        private long lastLongRunningFlowCleanTime = -1;

        public CleanerThread() {
            this.setName("ComplementRunnerManager-Cleaner-Thread");
            setDaemon(true);
        }

        public void shutdown() {
            this.shutdown = true;
            this.interrupt();
        }

        private boolean isFlowRunningLongerThan(final JobComplementVO flowComplementVO,
                final long complementMaxRunningTimeInMins) {
            return (!WorkflowInstanceStatus.FINISHED_STATUS.contains(flowComplementVO.getComplementStatus().getV()))
                    && flowComplementVO.getStartTime().toInstant(ZoneOffset.of("+8")).toEpochMilli() > 0
                    && TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - flowComplementVO.getStartTime()
                            .toInstant(ZoneOffset.of("+8")).toEpochMilli()) >= complementMaxRunningTimeInMins;
        }

        @Override
        public void run() {
            while (!this.shutdown) {
                synchronized (this) {
                    try {
                        ComplementRunnerManager.this.lastCleanerThreadCheckTime = System.currentTimeMillis();
                        ComplementRunnerManager.log.info("# of executing complements: " + getNumRunningFlows());

                        // Cleanup old stuff.
                        final long currentTime = System.currentTimeMillis();

                        if (this.complementMaxRunningTimeInMins > 0 && currentTime
                                - LONG_RUNNING_FLOW_KILLING_INTERVAL_MS > this.lastLongRunningFlowCleanTime) {
                            ComplementRunnerManager.log
                                    .info(String.format("Killing long jobs running longer than %s mins",
                                            this.complementMaxRunningTimeInMins));
                            for (final ComplementRunner complementRunner : ComplementRunnerManager.this.runningComplements
                                    .values()) {
                                if (isFlowRunningLongerThan(complementRunner.getFlowComplementVO(),
                                        this.complementMaxRunningTimeInMins)) {
                                    ComplementRunnerManager.log.info(String.format(
                                            "Killing job [id: %s, status: %s]. It has been running for %s mins",
                                            complementRunner.getFlowComplementVO().getWorkflowId(),
                                            complementRunner.getFlowComplementVO().getComplementStatus(),
                                            TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()
                                                    - complementRunner.getFlowComplementVO().getStartTime()
                                                            .toInstant(ZoneOffset.of("+8")).toEpochMilli())));
                                    complementRunner.kill();
                                }
                            }
                            this.lastLongRunningFlowCleanTime = currentTime;
                        }

                        wait(ComplementRunnerManager.RECENTLY_FINISHED_TIME_TO_LIVE);
                    } catch (final InterruptedException e) {
                        ComplementRunnerManager.log.info("Interrupted. Probably to shut down.");
                        Thread.currentThread().interrupt();
                    } catch (final Throwable t) {
                        ComplementRunnerManager.log.warn("Uncaught throwable, please look into why it is not caught",
                                t);
                    }
                }
            }
        }
    }
}
