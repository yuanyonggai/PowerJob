package tech.powerjob.server.core.workflow.complement;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.WorkflowInstanceInfoDO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;

/**
 * 阻塞等待状态变最终值
 */
@Slf4j
public class BlockExecutionStatus {
    private final AtomicBoolean unblock = new AtomicBoolean(false);
    private final Long instanceId;
    private final WorkflowInstanceInfoRepository workflowInstanceInfoRepository;
    private final InstanceInfoRepository instanceInfoRepository;
    private final int retryWaitSecond;

    public BlockExecutionStatus(Long instanceId, WorkflowInstanceInfoRepository workflowInstanceInfoRepository, InstanceInfoRepository instanceInfoRepository, int retryWaitSecond) {
        this.instanceId = instanceId;
        this.workflowInstanceInfoRepository = workflowInstanceInfoRepository;
        this.instanceInfoRepository = instanceInfoRepository;
        if (retryWaitSecond < 1) {
            this.retryWaitSecond = 1;
        } else {
            this.retryWaitSecond = retryWaitSecond;
        }
    }

    public static void stop() {
        Thread.currentThread().interrupt();
    }

    public WorkflowInstanceStatus blockOnWorkflowFinishedStatus() {
        WorkflowInstanceStatus workflowInstanceStatus = WorkflowInstanceStatus.WAITING;
        long firstLogTime = System.currentTimeMillis();
        long lastLogTime = firstLogTime;
        while (!WorkflowInstanceStatus.FINISHED_STATUS.contains(workflowInstanceStatus.getV())) {
            long currentLogTime = System.currentTimeMillis();
            if (currentLogTime - lastLogTime > 60 * 1000) {
                //一分钟打印一次信息
                log.info(" Waiting for the workflow in workflowInstanceId[" + instanceId + "] state to become the final state");
                lastLogTime = currentLogTime;
            }
            if (currentLogTime - firstLogTime > 24 * 60 * 60 * 1000) {
                //24小时
                log.info(" Waiting for the workflow in workflowInstanceId[" + instanceId + "] final state overtime");
                throw new PowerJobException("workflowInstanceId execute overtime!");
            }
            synchronized (this) {
                try {
                    this.wait(retryWaitSecond * 1000L);
                    if (unblock.get()) {
                        log.info("手动触发停止作业:" + instanceId);
                        return WorkflowInstanceStatus.STOPPED;
                    }
                } catch (final InterruptedException e) {
                    log.info("触发停止作业:" + instanceId);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("触发停止");
                }
            }

            // 检查中断标志
            if(Thread.interrupted()) {
                // 中断标志被设置,退出循环
                log.info("触发停止作业:" + instanceId);
                throw new RuntimeException("触发停止");
            }

            //根据工作流实例id查询数据库状态
            Optional<WorkflowInstanceInfoDO> wfInstanceInfoOpt = workflowInstanceInfoRepository.findByWfInstanceId(instanceId);
            if (!wfInstanceInfoOpt.isPresent()) {
                log.error("[WorkflowInstanceManager] can't find metadata by workflowInstanceId({}).", instanceId);
                throw new PowerJobException("[WorkflowInstanceManager] can't find metadata by workflowInstanceId.");
            }
            WorkflowInstanceInfoDO wfInstanceInfo = wfInstanceInfoOpt.get();
            workflowInstanceStatus = WorkflowInstanceStatus.of(wfInstanceInfo.getStatus());    
        }
        log.info("the workflow in workflowInstanceId[" + instanceId + "]  become the final state " + workflowInstanceStatus.getDes());
        return workflowInstanceStatus;
    }

    public InstanceStatus blockOnJobFinishedStatus() {
        InstanceStatus instanceStatus = InstanceStatus.WAITING_DISPATCH;
        long firstLogTime = System.currentTimeMillis();
        long lastLogTime = firstLogTime;
        while (!InstanceStatus.FINISHED_STATUS.contains(instanceStatus.getV())) {
            long currentLogTime = System.currentTimeMillis();
            if (currentLogTime - lastLogTime > 60 * 1000) {
                //一分钟打印一次信息
                log.info(" Waiting for the job in instanceId[" + instanceId + "] state to become the final state");
                lastLogTime = currentLogTime;
            }
            if (currentLogTime - firstLogTime > 24 * 60 * 60 * 1000) {
                //24小时
                log.info(" Waiting for the job in instanceId[" + instanceId + "] final state overtime");
                throw new PowerJobException("instanceId execute overtime!");
            }
            synchronized (this) {
                try {
                    this.wait(retryWaitSecond * 1000L);
                    if (unblock.get()) {
                        log.info("手动触发停止作业:" + instanceId);
                        return InstanceStatus.STOPPED;
                    }
                } catch (final InterruptedException e) {
                    log.info("触发停止作业:" + instanceId);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("触发停止");
                }
            }

            // 检查中断标志
            if(Thread.interrupted()) {
                // 中断标志被设置,退出循环
                log.info("触发停止作业:" + instanceId);
                throw new RuntimeException("触发停止");
            }

            //根据实例id查询数据库状态
            InstanceInfoDO instanceInfo = instanceInfoRepository.findByInstanceId(instanceId);
            if (instanceInfo == null) {
                log.error("[InstanceManager] can't find metadata by instanceId({}).", instanceId);
                throw new PowerJobException("[InstanceManager] can't find metadata by instanceId.");
            }
            instanceStatus = InstanceStatus.of(instanceInfo.getStatus());    
        }
        log.info("the job in instanceId[" + instanceId + "]  become the final state " + instanceStatus.getDes());
        return instanceStatus;
    }

    public void unblock() {
        synchronized (this) {
            unblock.set(true);
            this.notifyAll();
        }
    }
}

