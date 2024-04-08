package tech.powerjob.server.core.workflow.complement;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.server.persistence.remote.model.WorkflowInstanceInfoDO;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 阻塞等待状态变最终值
 */
@Slf4j
public class BlockFlowExecutionStatus {
    private final AtomicBoolean unblock = new AtomicBoolean(false);
    private final Long wfInstanceId;
    private final WorkflowInstanceInfoRepository workflowInstanceInfoRepository;
    private final int retryWaitSecond;

    public BlockFlowExecutionStatus(Long wfInstanceId, WorkflowInstanceInfoRepository workflowInstanceInfoRepository, int retryWaitSecond) {
        this.wfInstanceId = wfInstanceId;
        this.workflowInstanceInfoRepository = workflowInstanceInfoRepository;
        if (retryWaitSecond < 1) {
            this.retryWaitSecond = 1;
        } else {
            this.retryWaitSecond = retryWaitSecond;
        }
    }

    public static void stop() {
        Thread.currentThread().interrupt();
    }

    public WorkflowInstanceStatus blockOnFinishedStatus() {
        // Status status = Status.READY;
        WorkflowInstanceStatus workflowInstanceStatus = WorkflowInstanceStatus.WAITING;
        long firstLogTime = System.currentTimeMillis();
        long lastLogTime = firstLogTime;
        while (!WorkflowInstanceStatus.FINISHED_STATUS.contains(workflowInstanceStatus.getV())) {
            long currentLogTime = System.currentTimeMillis();
            if (currentLogTime - lastLogTime > 60 * 1000) {
                //一分钟打印一次信息
                log.info(" Waiting for the workflow in workflowInstanceId[" + wfInstanceId + "] state to become the final state");
                lastLogTime = currentLogTime;
            }
            if (currentLogTime - firstLogTime > 24 * 60 * 60 * 1000) {
                //24小时
                log.info(" Waiting for the workflow in workflowInstanceId[" + wfInstanceId + "] final state overtime");
                throw new PowerJobException("workflowInstanceId execute overtime!");
            }
            synchronized (this) {
                try {
                    this.wait(retryWaitSecond * 1000L);
                    if (unblock.get()) {
                        log.info("手动触发停止作业:" + wfInstanceId);
                        return WorkflowInstanceStatus.STOPPED;
                    }
                } catch (final InterruptedException e) {
                    log.info("触发停止作业:" + wfInstanceId);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("触发停止");
                }
            }

            // 检查中断标志
            if(Thread.interrupted()) {
                // 中断标志被设置,退出循环
                log.info("触发停止作业:" + wfInstanceId);
                throw new RuntimeException("触发停止");
            }

            //根据工作流实例id查询数据库状态
            Optional<WorkflowInstanceInfoDO> wfInstanceInfoOpt = workflowInstanceInfoRepository.findByWfInstanceId(wfInstanceId);
            if (!wfInstanceInfoOpt.isPresent()) {
                log.error("[WorkflowInstanceManager] can't find metadata by workflowInstanceId({}).", wfInstanceId);
                throw new PowerJobException("[WorkflowInstanceManager] can't find metadata by workflowInstanceId.");
            }
            WorkflowInstanceInfoDO wfInstanceInfo = wfInstanceInfoOpt.get();
            workflowInstanceStatus = WorkflowInstanceStatus.of(wfInstanceInfo.getStatus());    
        }
        log.info("the workflow in workflowInstanceId[" + wfInstanceId + "]  become the final state " + workflowInstanceStatus.getDes());
        return workflowInstanceStatus;
    }

    public void unblock() {
        synchronized (this) {
            unblock.set(true);
            this.notifyAll();
        }
    }
}

