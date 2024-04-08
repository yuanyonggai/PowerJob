package tech.powerjob.server.core.workflow.complement.vo;

import lombok.Data;
import tech.powerjob.common.enums.WorkflowInstanceStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Data
public final class FlowComplementVO {
    //工作流id
    private Long workflowId;

    //应用id
    private Long appId;

    //补数开始日期
    private LocalDate startDataDate;

    //补数结束日期
    private LocalDate endDataDate;

    //当前数据日期<只有串行补数时有效>
    private LocalDate currentDataDate;

    //补数状态
    private WorkflowInstanceStatus complementStatus;

    //创建时间
    private LocalDateTime addTime;

    //修改时间
    private LocalDateTime updateTime;

    //开始时间
    private LocalDateTime startTime;

    //结束时间
    private LocalDateTime endTime;

    //是否并行
    private boolean parallel;

    //自定义参数
    private Map<String, String> flowData;

    public FlowComplementVO() {
    }

    public FlowComplementVO(Long workflowId, LocalDate startDataDate, LocalDate endDataDate) {
        this.workflowId = workflowId;
        this.startDataDate = startDataDate;
        this.endDataDate = endDataDate;
        this.complementStatus = WorkflowInstanceStatus.WAITING;
    }

    public Map toMap() {
        return new HashMap<>();
    }

    public FlowComplementVO getFlowComplementVO() {
        FlowComplementVO flowComplementVO = new FlowComplementVO();
        flowComplementVO.setWorkflowId(this.workflowId);
        flowComplementVO.setStartDataDate(this.startDataDate);
        flowComplementVO.setEndDataDate(this.endDataDate);
        flowComplementVO.setComplementStatus(this.complementStatus);
        return flowComplementVO;
    }
}
