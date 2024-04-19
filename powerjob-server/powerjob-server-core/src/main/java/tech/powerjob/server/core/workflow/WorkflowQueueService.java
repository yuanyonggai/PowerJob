package tech.powerjob.server.core.workflow;

import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.server.extension.LockService;
import tech.powerjob.server.persistence.remote.model.WorkflowInstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.WorkflowQueueInfoDO;
import tech.powerjob.server.persistence.remote.repository.WorkflowAllRunInfoRepository;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;
import tech.powerjob.server.persistence.remote.repository.WorkflowQueueInfoRepository;

/**
 * 工作流队列服务，接收外部现场检查提数消息
 *
 * @author yyg
 * @since 2024/4/18
 */
@Service
@Slf4j
public class WorkflowQueueService {

    private static final String WORKFLOW_QUEUE_LOCK = "workflow_queue_lock";
    private static final int SERVER_INIT_LOCK_MAX_TIME = 10 * 60 * 1000;// 10分钟
    public static final String GDH_TYPE_CHECK = "check";// 消息类型
    public static final Long WORKFLOW_ID_CHECK = 8L;// 现场检查任务流id TODO，如何整理一个文档，定义下

    public WorkflowQueueService(LockService lockService, WorkflowQueueInfoRepository workflowQueueInfoRepository,
            WorkflowAllRunInfoRepository workflowAllRunInfoRepository,
            WorkflowInstanceInfoRepository workflowInstanceInfoRepository, WorkflowService workflowService) {

        // 创建并启动后台线程
        Thread backgroundThread = new Thread(() -> {
            log.info("[WorkflowQueueService] thread is running...");
            while (true) {
                CommonUtils.easySleep(1 * 60 * 1000);// 1分钟
                if (!lockService.tryLock(WORKFLOW_QUEUE_LOCK, SERVER_INIT_LOCK_MAX_TIME)) {
                    log.info("[WorkflowQueueService] waiting for lock for 5 minutes: {}", WORKFLOW_QUEUE_LOCK);
                    CommonUtils.easySleep(5 * 60 * 1000);// 5分钟
                }

                try {
                    // 排序
                    Sort sort = Sort.by(Sort.Direction.ASC, "gmtCreate");

                    // 取一条状态为就绪的工作流队列对象
                    Pageable pageable = PageRequest.of(0, 1, sort);
                    //TODO:测试是否有缓存，比如一个jvm修改了，另一个查询，是否数据一致?
                    List<WorkflowQueueInfoDO> workflowQueueInfoList = workflowQueueInfoRepository.findByGdhStatus(
                            WorkflowInstanceStatus.WAITING.getV(),
                            pageable);
                    for (WorkflowQueueInfoDO workflowQueueInfoDO : workflowQueueInfoList) {
                        // 如果是check,还要看当天主流程是否跑完成！
                        if (GDH_TYPE_CHECK.equalsIgnoreCase(workflowQueueInfoDO.getGdhType())) {
                            long count = workflowAllRunInfoRepository.countByCreateDate(DateUtil.today());
                            if (count <= 0) {// 如果找不到记录，代表今天主任务流没有跑批完成
                                // "当天主流程未完成，(需要在主任务流最后加一个sql任务，当每天跑批后向表sch_workflow_all_run_info插入数据，代表可以跑现场检查提数。)";
                            } else {
                                // 调用前判断下，如果有在执行，等待2分钟
                                int instanceConcurrency = workflowInstanceInfoRepository.countByWorkflowIdAndStatusIn(
                                        WORKFLOW_ID_CHECK,
                                        WorkflowInstanceStatus.GENERALIZED_RUNNING_STATUS);
                                if (instanceConcurrency >= 1) {
                                    log.info("[WorkflowQueueService] 现场检查任务流正在执行中!等待2分钟..");
                                    CommonUtils.easySleep(120000);
                                } else {
                                    if (workflowQueueInfoDO.getWfInstanceId() == null) {// 如果工作流实例为空才调用，否则重复调用了                                        
                                        log.info("[WorkflowQueueService] 开始调用现场检查跑批..");
                                        // TODO: appId
                                        Long wfInstanceId = workflowService.runWorkflow(WORKFLOW_ID_CHECK, 1L,
                                                workflowQueueInfoDO.getFlowRunDto(),
                                                null);
                                        workflowQueueInfoDO.setWfInstanceId(wfInstanceId);
                                        workflowQueueInfoRepository.saveAndFlush(workflowQueueInfoDO);
                                    } else {// 如果不等于空，查询一下工作流状态，同步更改一下（注意：此时，这个状态可能是失败、手动停止、成功）
                                        WorkflowInstanceInfoDO wfInstance = workflowInstanceInfoRepository
                                                .findByWfInstanceId(workflowQueueInfoDO.getWfInstanceId()).orElse(null);
                                        if (wfInstance == null) {
                                            log.error("workflow instance({}) is not exist!",
                                                    workflowQueueInfoDO.getWfInstanceId());
                                            throw new PowerJobException("invalid workflow instance id "
                                                    + workflowQueueInfoDO.getWfInstanceId());
                                        }
                                        workflowQueueInfoDO.setGdhStatus(wfInstance.getStatus().intValue());
                                        workflowQueueInfoRepository.saveAndFlush(workflowQueueInfoDO);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("[WorkflowQueueService] failed", e);
                    throw e;
                } finally {
                    lockService.unlock(WORKFLOW_QUEUE_LOCK);
                }
            }
        });

        // 将线程标记为后台线程
        backgroundThread.setDaemon(true);
        backgroundThread.setName("WorkflowQueueService Thread");

        // 启动线程
        backgroundThread.start();
    }
}