package tech.powerjob.server.core.workflow.complement;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.model.RunParams;
import tech.powerjob.server.core.service.JobService;
import tech.powerjob.server.core.workflow.WorkflowService;
import tech.powerjob.server.core.workflow.complement.event.Event;
import tech.powerjob.server.core.workflow.complement.event.EventData;
import tech.powerjob.server.core.workflow.complement.event.EventHandler;
import tech.powerjob.server.core.workflow.complement.event.EventListener;
import tech.powerjob.server.core.workflow.complement.event.EventReporter;
import tech.powerjob.server.core.workflow.complement.event.EventType;
import tech.powerjob.server.core.workflow.complement.vo.JobComplementVO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;

/**
 * Class that handles complement data
 */
public class ComplementRunner extends EventHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ComplementRunner.class);

    // 人工补数任务信息存储
    private JobComplementVO flowComplementVO;
    // Sync object for queuing
    private final Object mainSyncObj = new Object();

    private final ComplementRunnerEventListener complementRunnerEventListener = new ComplementRunnerEventListener();

    private final EventReporter eventReporter;

    private Thread complementRunnerThread;

    private WorkflowService workflowService;
    private JobService jobService;
    private volatile boolean flowComplementKilled = false;

    private BlockExecutionStatus blockFlowStatus = null;

    private final WorkflowInstanceInfoRepository workflowInstanceInfoRepository;
    private final InstanceInfoRepository instanceInfoRepository;

    /**
     * Constructor
     */
    public ComplementRunner(final JobComplementVO flowComplementVO, final WorkflowService workflowService,
            final JobService jobService,
            final WorkflowInstanceInfoRepository workflowInstanceInfoRepository,
            final InstanceInfoRepository instanceInfoRepository, final EventReporter eventReporter) {
        this.flowComplementVO = flowComplementVO;
        this.jobService = jobService;
        this.workflowService = workflowService;
        this.workflowInstanceInfoRepository = workflowInstanceInfoRepository;
        this.instanceInfoRepository = instanceInfoRepository;
        // Add the listener only if a non-null eventReporter is available.
        if (eventReporter != null) {
            this.addListener(this.complementRunnerEventListener);
        }
        this.eventReporter = eventReporter;
    }

    @Override
    public void run() {
        try {
            setupFlowExecution();
            this.fireEventListeners(Event.create(this, EventType.COMPLEMENT_STARTED,
                    new EventData(this.getFlowComplementVO().toMap())));
            runFlowComplement();
        } catch (final Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof InterruptedException) {
                logger.info("作业流id or jobId:{}补数被强制中断", flowComplementVO.getWorkflowId());
            } else {
                logger.info("作业流id or jobId:{} 异常：{}", flowComplementVO.getWorkflowId(), e);
            }
            this.flowComplementVO.setWorkflowInstanceStatus(WorkflowInstanceStatus.STOPPED);
            this.flowComplementVO.setInstanceStatus(InstanceStatus.STOPPED);
        } finally {
            try {
                this.flowComplementVO.setEndTime(LocalDateTime.now());
                logger.info("Setting end time for flowComplementVO " + this.flowComplementVO.getWorkflowId() + " to "
                        + LocalDateTime.now());
            } finally {
                this.fireEventListeners(Event.create(this, EventType.COMPLEMENT_FINISHED,
                        new EventData(this.getFlowComplementVO().toMap())));
            }
            // 关闭其他
        }
    }

    //
    private void setupFlowExecution() {
        this.complementRunnerThread = Thread.currentThread();
        this.complementRunnerThread
                .setName("ComplementRunner[workflowId or jobId:" + this.flowComplementVO.getWorkflowId() + "]");
    }

    /**
     * Main method that executes the jobs.
     */
    private void runFlowComplement() throws Exception {
        logger.info("starting flow complement ");
        runReadyComplement();
        logger.info("finished flow complement ");
    }

    private void runReadyComplement() throws Exception {
        LocalDate startDate = this.flowComplementVO.getStartDataDate();
        LocalDate endDate = this.flowComplementVO.getEndDataDate();

        long distance = ChronoUnit.DAYS.between(startDate, endDate);

        if (distance < 0) {
            throw new PowerJobException(
                    "workflowId or jobId:" + this.flowComplementVO.getWorkflowId() + " complement not success!");
        }
        for (long i = 0; i <= distance && flowComplementKilled == false; i++) {
            try {
                this.flowComplementVO.setCurrentDataDate(startDate.plusDays(i));

                RunParams workflowParams = new RunParams();
                workflowParams.setDataDate(
                        this.flowComplementVO.getCurrentDataDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                workflowParams.setDataDateStart(
                        this.flowComplementVO.getStartDataDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                workflowParams.setDataDateEnd(
                        this.flowComplementVO.getEndDataDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                String runDateParams = JSON.toJSONString(workflowParams);

                if (this.flowComplementVO.isWorkflow()) {
                    Long wfInstanceId = workflowService.runWorkflow(flowComplementVO.getWorkflowId(),
                            flowComplementVO.getAppId(), runDateParams, 0L);
                    blockFlowStatus = new BlockExecutionStatus(wfInstanceId, workflowInstanceInfoRepository, null, 20);
                    WorkflowInstanceStatus status = blockFlowStatus.blockOnWorkflowFinishedStatus();
                    if (!status.equals(WorkflowInstanceStatus.SUCCEED)) {
                        throw new PowerJobException("workflowInstanceId [" + wfInstanceId + "] execute not success!");
                    }
                } else {
                    Long instanceId = jobService.runJob(flowComplementVO.getAppId(), flowComplementVO.getWorkflowId(),
                            this.flowComplementVO.getInstanceParams(),
                            runDateParams, 0L);
                    blockFlowStatus = new BlockExecutionStatus(instanceId, null, instanceInfoRepository, 20);
                    InstanceStatus status = blockFlowStatus.blockOnJobFinishedStatus();
                    if (!status.equals(InstanceStatus.SUCCEED)) {
                        throw new PowerJobException("instanceId [" + instanceId + "] execute not success!");
                    }
                }

            } catch (Exception e) {
                throw new PowerJobException(e);
            }
        }
    }

    public void kill() {
        synchronized (this.mainSyncObj) {
            if (this.flowComplementKilled) {
                return;
            }
            logger.info("Kill has been called on complement workflowId: " + flowComplementVO.getWorkflowId());
            this.flowComplementVO.setWorkflowInstanceStatus(WorkflowInstanceStatus.STOPPED);
            // If the flowComplementVO is paused, then we'll also unpause
            this.flowComplementKilled = true;

            if (blockFlowStatus != null) {
                blockFlowStatus.unblock();
            }
        }
        interrupt();
    }

    private void interrupt() {
        this.complementRunnerThread.interrupt();
    }

    public boolean isKilled() {
        return this.flowComplementKilled;
    }

    public JobComplementVO getFlowComplementVO() {
        return this.flowComplementVO.getFlowComplementVO();
    }

    // Class helps report the flowComplementVO start and stop events.
    private class ComplementRunnerEventListener implements EventListener {

        public ComplementRunnerEventListener() {
        }

        private synchronized Map<String, String> getFlowComplementMetadata(final ComplementRunner complementRunner) {
            final JobComplementVO flowComplementVO = complementRunner.getFlowComplementVO();

            final Map<String, String> metaData = new HashMap<>();
            metaData.put("complementKey", String.valueOf(flowComplementVO.getWorkflowId()));
            metaData.put("startTime", String.valueOf(flowComplementVO.getStartTime()));
            return metaData;
        }

        @Override
        public synchronized void handleEvent(final Event event) {
            if (event.getType() == EventType.COMPLEMENT_STARTED) {
                final ComplementRunner complementRunner = (ComplementRunner) event.getRunner();
                final JobComplementVO flowComplementVO = complementRunner.getFlowComplementVO();
                ComplementRunner.logger
                        .info("flow complement started workflowId or jobId: " + flowComplementVO.getWorkflowId());
                ComplementRunner.this.eventReporter.report(event.getType(),
                        getFlowComplementMetadata(complementRunner));
            } else if (event.getType() == EventType.COMPLEMENT_FINISHED) {
                final ComplementRunner complementRunner = (ComplementRunner) event.getRunner();
                final JobComplementVO flowComplementVO = complementRunner.getFlowComplementVO();
                ComplementRunner.logger
                        .info("flow complement ended workflowId or jobId: " + flowComplementVO.getWorkflowId());
                final Map<String, String> flowMetadata = getFlowComplementMetadata(complementRunner);
                flowMetadata.put("endTime", String.valueOf(flowComplementVO.getEndTime()));
                if (flowComplementVO.isWorkflow()) {
                    flowMetadata.put("ComplementStatus", String.valueOf(flowComplementVO.getWorkflowInstanceStatus()));
                } else {
                    flowMetadata.put("ComplementStatus", String.valueOf(flowComplementVO.getInstanceStatus()));
                }
                ComplementRunner.this.eventReporter.report(event.getType(), flowMetadata);
            }
        }
    }
}
