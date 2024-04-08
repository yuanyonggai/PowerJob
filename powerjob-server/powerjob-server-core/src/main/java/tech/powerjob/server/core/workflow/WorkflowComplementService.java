package tech.powerjob.server.core.workflow;

import java.time.LocalDate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.server.core.workflow.complement.ComplementRunnerManager;
import tech.powerjob.server.core.workflow.complement.vo.FlowComplementVO;
import tech.powerjob.server.persistence.remote.repository.WorkflowInstanceInfoRepository;
import tech.powerjob.server.remote.server.redirector.DesignateServer;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.server.core.lock.UseCacheLock;

/**
 * Workflow 补数服务
 *
 * @author yyg
 * @since 2024/4/7
 */
@Service
@Slf4j
public class WorkflowComplementService {

    @Autowired
    @Lazy
    private ComplementRunnerManager complementRunnerManager;

    @Autowired
    private WorkflowInstanceInfoRepository workflowInstanceInfoRepository;

    /**
     * 运行补数
     *
     * @param dataDateStart 补数开始日期
     * @param dataDateEnd   补数结束日期
     * @return 工作流ID
     */
    @DesignateServer
    @UseCacheLock(type = "runComplement", key = "#workflowId", concurrencyLevel = 1024)
    public Long runComplement(Long appId, Long workflowId, LocalDate dataDateStart, LocalDate dataDateEnd) {
        // 判断是否在运行,如果是抛出异常
        int instanceConcurrency = workflowInstanceInfoRepository.countByWorkflowIdAndStatusIn(workflowId, WorkflowInstanceStatus.GENERALIZED_RUNNING_STATUS);
        if (instanceConcurrency >= 1) {
            log.error("[WorkflowComplementService] can't run at the same time by workflowId({}).", workflowId);
            throw new PowerJobException("此任务流正在执行中!");
        }
        FlowComplementVO flowComplementVO = new FlowComplementVO();
        flowComplementVO.setAppId(appId);
        flowComplementVO.setWorkflowId(workflowId);
        flowComplementVO.setStartDataDate(dataDateStart);
        flowComplementVO.setEndDataDate(dataDateEnd);
        complementRunnerManager.submitComplement(flowComplementVO);
        return workflowId;
    }
}
