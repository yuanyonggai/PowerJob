package tech.powerjob.server.core.workflow;

import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.server.extension.LockService;
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
    private static final int SERVER_INIT_LOCK_MAX_TIME = 15000;
    public static final String GDH_TYPE_CHECK = "check";// 消息类型
    public static final Long WORKFLOW_ID_CHECK = 8L;// 现场检查任务流id TODO，如何整理一个文档，定义下

    public WorkflowQueueService(LockService lockService, WorkflowQueueInfoRepository workflowQueueInfoRepository,
            WorkflowAllRunInfoRepository workflowAllRunInfoRepository,
            WorkflowInstanceInfoRepository workflowInstanceInfoRepository, WorkflowService workflowService) {
        log.info("WorkflowQueueService startting..");
        while (true) {
            if (!lockService.tryLock(WORKFLOW_QUEUE_LOCK, SERVER_INIT_LOCK_MAX_TIME)) {
                log.info("[WorkflowQueueService] waiting for lock: {}", WORKFLOW_QUEUE_LOCK);
                CommonUtils.easySleep(5000);
            }

            // 排序
            Sort sort = Sort.by(Sort.Direction.ASC, "gmtCreate");

            boolean updateFlag = false;

            // 取一条状态为就绪的工作流队列对象
            Pageable pageable = PageRequest.of(0, 1, sort);
            List<WorkflowQueueInfoDO> workflowQueueInfoList = workflowQueueInfoRepository.findByGdhStatus(0, pageable);
            for (WorkflowQueueInfoDO workflowQueueInfoDO : workflowQueueInfoList) {
                // 如果是check,还要看当天主流程是否跑完成！
                if (GDH_TYPE_CHECK.equalsIgnoreCase(workflowQueueInfoDO.getGdhType())) {
                    long count = workflowAllRunInfoRepository.countByCreateDate(DateUtil.today());
                    if (count <= 0) {// 如果找不到记录，代表今天主任务流没有跑批完成
                        if (!updateFlag) {
                            String info = "当天主流程未完成，(需要在主任务流最后加一个sql任务，当每天跑批后向表sch_workflow_all_run_info插入数据，代表可以跑现场检查提数。)";
                            log.info("WorkflowQueueService error:{}", info);
                            workflowQueueInfoDO.setRunDesc(info);
                            workflowQueueInfoRepository.saveAndFlush(workflowQueueInfoDO);
                            updateFlag = true;
                        }
                    } else {                        
                        // 调用前判断下，如果有在执行，等待2分钟
                        int instanceConcurrency = workflowInstanceInfoRepository.countByWorkflowIdAndStatusIn(
                                WORKFLOW_ID_CHECK,
                                WorkflowInstanceStatus.GENERALIZED_RUNNING_STATUS);
                        if (instanceConcurrency >= 1) {
                            log.info("[WorkflowQueueService] 现场检查任务流正在执行中!等待2分钟..");
                            CommonUtils.easySleep(120000);
                        } else {
                            log.info("[WorkflowQueueService] 开始调用现场检查跑批..");
                            //TODO: appId                            
                            workflowService.runWorkflow(WORKFLOW_ID_CHECK, 1L, workflowQueueInfoDO.getFlowRunDto(), null);
                            updateFlag = false;
                        }
                    }
                }
            }
        }
    }
}