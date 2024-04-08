package tech.powerjob.server.core.workflow.complement;

import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.model.WorkflowParams;
import tech.powerjob.server.core.workflow.WorkflowService;
import tech.powerjob.server.core.workflow.complement.event.*;
import com.alibaba.fastjson.JSON;
import tech.powerjob.server.core.workflow.complement.vo.FlowComplementVO;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;


/**
 * Class that handles complement  data
 */
public class ComplementRunner extends EventHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ComplementRunner.class);

    //人工补数任务信息存储
    private FlowComplementVO flowComplementVO;
    // Sync object for queuing
    private final Object mainSyncObj = new Object();

    private final ComplementRunnerEventListener complementRunnerEventListener = new ComplementRunnerEventListener();

    private final EventReporter eventReporter;   

    private Thread complementRunnerThread;

    private WorkflowService workflowService;

    private volatile boolean flowComplementKilled = false;

    private BlockFlowExecutionStatus blockFlowStatus = null;

    private final WorkflowInstanceInfoRepository workflowInstanceInfoRepository;

    /**
     * Constructor
     */
    public ComplementRunner(final FlowComplementVO flowComplementVO, final WorkflowService workflowService, final WorkflowInstanceInfoRepository workflowInstanceInfoRepository, final EventReporter eventReporter) {
        this.flowComplementVO = flowComplementVO;
        this.workflowService = workflowService;
        this.workflowInstanceInfoRepository = workflowInstanceInfoRepository;
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
            // updateFlowComplement();
            this.fireEventListeners(Event.create(this, EventType.COMPLEMENT_STARTED, new EventData(this.getFlowComplementVO().toMap())));
            runFlowComplement();
        } catch (final Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof InterruptedException) {
                logger.info("作业流id:{}补数被强制中断", flowComplementVO.getWorkflowId());
            } else {
                logger.info("作业流id:{} 异常：{}", flowComplementVO.getWorkflowId(), e);
            }
            this.flowComplementVO.setComplementStatus(WorkflowInstanceStatus.STOPPED);
        } finally {
            try {
                this.flowComplementVO.setEndTime(LocalDateTime.now());
                logger.info("Setting end time for flowComplementVO " + this.flowComplementVO.getWorkflowId() + " to " + LocalDateTime.now());
                //closeLogger();
                // updateFlowComplement();
            } finally {
                this.fireEventListeners(Event.create(this, EventType.COMPLEMENT_FINISHED, new EventData(this.getFlowComplementVO().toMap())));
            }
            //关闭其他
        }
    }

    //
    private void setupFlowExecution() {
        this.complementRunnerThread = Thread.currentThread();
        this.complementRunnerThread.setName("ComplementRunner[workflowId:" + this.flowComplementVO.getWorkflowId() + "]");
    }

    //
//     public synchronized void updateFlowComplement() {

//         LocalDateTime now = LocalDateTime.now();
//         this.flowComplementVO.setUpdateTime(now);//更新内存时间
//         try {
//             //更新数据库时间和状态
//             FlowComplement flowComplement = new FlowComplement();
//             flowComplement.setFlowComplementKey(flowComplementVO.getFlowComplementKey());
//             flowComplement.setUpdateTime(now);

// //            if (flowComplementVO.getStartTime() != null) {
// //                flowComplement.setStartTime(now);
// //            }
//             if (flowComplementVO.getEndTime() != null) {
//                 flowComplement.setEndTime(now);
//             }
//             if (flowComplementVO.getCurrentBusinessDate() != null) {
//                 flowComplement.setCurrentBusinessDate(flowComplementVO.getCurrentBusinessDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
//             }
//             if (flowComplementVO.getComplementStatus() != null) {
//                 flowComplement.setComplementStatus(flowComplementVO.getComplementStatus().toString());
//             }
//             flowComplementService.updateById(flowComplement);

//         } catch (final Exception e) {

//             if (ExceptionUtil.getRootCause(e) instanceof InterruptedException) {
//                 logger.info("人工作业实例 flowComplementKey:{} 被强制中断", flowComplementVO.getFlowComplementKey());
//             } else {
//                 logger.info("人工作业实例 flowComplementKey:{}更新异常：{}", flowComplementVO.getFlowComplementKey(), e.getMessage());
//             }
//         }
//     }

    /**
     * Main method that executes the jobs.
     */
    private void runFlowComplement() throws Exception {
        logger.info("starting flow complement ");
        runReadyComplement();
        // updateFlowComplement();
        logger.info("finished flow complement ");
    }

    private void runReadyComplement() throws Exception {

        // if (this.flowComplementVO.getComplementStatus().isTerminal()) {
        //     throw new BusinessException("flowComplement:" + this.flowComplementVO.getFlowComplementKey() + " not success!");
        // }

        //FlowInfo flowInfo = flowInfoService.getById(flowComplementVO.getFlowKey());

        LocalDate startDate = this.flowComplementVO.getStartDataDate();
        LocalDate endDate = this.flowComplementVO.getEndDataDate();

        long distance = ChronoUnit.DAYS.between(startDate, endDate);

        if (distance < 0) {
            throw new PowerJobException("workflowId:" + this.flowComplementVO.getWorkflowId() + " complement not success!");
        }
        for (long i = 0; i <= distance && flowComplementKilled == false; i++) {            
            try {
                // this.flowComplementVO.setComplementStatus(Status.RUNNING);
                // this.flowComplementVO.setStartTime(LocalDateTime.now());
                this.flowComplementVO.setCurrentDataDate(startDate.plusDays(i));
                //updateFlowComplement();
                //
//                if (StrUtil.isNotEmpty(flowInfo.getSlotIdKey())) {
//                    logger.info("try to get slotId");
//                    slotId = distributedJobService.checkInSlot(distributedJobService.getCommonDistributedConfig().getSlotIdMap().get(flowInfo.getSlotIdKey()), flowInfo.getFlowKey());
//                    logger.info("slotId is " + slotId);
//                }         

                WorkflowParams workflowParams = new WorkflowParams();
                String dataDate = this.flowComplementVO.getCurrentDataDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));                
                workflowParams.setDataDateStart(dataDate);
                workflowParams.setDataDateEnd(dataDate);
                String initParams = JSON.toJSONString(workflowParams);
                Long wfInstanceId = workflowService.runWorkflow(flowComplementVO.getWorkflowId(), flowComplementVO.getAppId(), initParams, 0L);

                //flowExecutionMap.put(flowExecutionKey, Status.RUNNING);

                blockFlowStatus = new BlockFlowExecutionStatus(wfInstanceId, workflowInstanceInfoRepository, 20);
                WorkflowInstanceStatus status = blockFlowStatus.blockOnFinishedStatus();
                // flowExecutionMap.put(flowExecutionKey, status);
                if (!status.equals(WorkflowInstanceStatus.SUCCEED)) {
                    // throw new BusinessException("flowExecutionKey:" + flowExecutionKey + " not success!");
                    throw new PowerJobException("workflowInstanceId [" + wfInstanceId + "] execute not success!");
                }
            } catch (Exception e) {
                throw new PowerJobException(e);
            } /* finally {
                if (slotId != null) {
                    distributedJobService.checkOutSlot(slotId);
                }
            } */

        }
        //获取最终状态
        //this.flowComplementVO.setComplementStatus(Status.SUCCESS);

    }

    public void kill() {
        synchronized (this.mainSyncObj) {
            if (this.flowComplementKilled) {
                return;
            }
            logger.info("Kill has been called on complement workflowId: " + flowComplementVO.getWorkflowId());
            this.flowComplementVO.setComplementStatus(WorkflowInstanceStatus.STOPPED);
            // If the flowComplementVO is paused, then we'll also unpause
            this.flowComplementKilled = true;

            if (blockFlowStatus != null) {
                blockFlowStatus.unblock();
            }

            // updateFlowComplement();
        }
        interrupt();
    }

    private void interrupt() {
        this.complementRunnerThread.interrupt();
    }

    //
    public boolean isKilled() {
        return this.flowComplementKilled;
    }

    //
    public FlowComplementVO getFlowComplementVO() {
        return this.flowComplementVO.getFlowComplementVO();
    }

    // Class helps report the flowComplementVO start and stop events.
    private class ComplementRunnerEventListener implements EventListener {

        public ComplementRunnerEventListener() {
        }

        private synchronized Map<String, String> getFlowComplementMetadata(final ComplementRunner complementRunner) {
            final FlowComplementVO flowComplementVO = complementRunner.getFlowComplementVO();

            final Map<String, String> metaData = new HashMap<>();
            // metaData.put("flowName", flowComplementVO.getFlowName());
            metaData.put("complementKey", String.valueOf(flowComplementVO.getWorkflowId()));
            metaData.put("startTime", String.valueOf(flowComplementVO.getStartTime()));
            return metaData;
        }

        @Override
        public synchronized void handleEvent(final Event event) {
            if (event.getType() == EventType.COMPLEMENT_STARTED) {
                final ComplementRunner complementRunner = (ComplementRunner) event.getRunner();
                final FlowComplementVO flowComplementVO = complementRunner.getFlowComplementVO();
                ComplementRunner.logger.info("flow complement started workflowId: " + flowComplementVO.getWorkflowId());
                ComplementRunner.this.eventReporter.report(event.getType(), getFlowComplementMetadata(complementRunner));
            } else if (event.getType() == EventType.COMPLEMENT_FINISHED) {
                final ComplementRunner complementRunner = (ComplementRunner) event.getRunner();
                final FlowComplementVO flowComplementVO = complementRunner.getFlowComplementVO();
                ComplementRunner.logger.info("flow complement ended workflowId: " + flowComplementVO.getWorkflowId());
                final Map<String, String> flowMetadata = getFlowComplementMetadata(complementRunner);
                flowMetadata.put("endTime", String.valueOf(flowComplementVO.getEndTime()));
                flowMetadata.put("ComplementStatus", String.valueOf(flowComplementVO.getComplementStatus()));
                ComplementRunner.this.eventReporter.report(event.getType(), flowMetadata);
            }
        }
    }
}
