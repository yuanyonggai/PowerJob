package tech.powerjob.server.core.workflow.complement.vo;

import lombok.Data;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.WorkflowInstanceStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Data
public final class JobComplementVO {
    //实际存的是工作流id or jobId
    private Long workflowId;

    //应用id
    private Long appId;

    //补数开始日期
    private LocalDate startDataDate;

    //补数结束日期
    private LocalDate endDataDate;

    //当前数据日期<只有串行补数时有效>
    private LocalDate currentDataDate;

    //工作流补数状态
    private WorkflowInstanceStatus workflowInstanceStatus;
    
    //任务补数状态
    private InstanceStatus instanceStatus;

    //创建时间
    private LocalDateTime addTime;

    //修改时间
    private LocalDateTime updateTime;

    //开始时间
    private LocalDateTime startTime;

    //结束时间
    private LocalDateTime endTime;    

    //是否工作流
    private boolean isWorkflow = true;

    //实例参数
    private String instanceParams;

    public JobComplementVO() {
    }

    public JobComplementVO(Long workflowId, LocalDate startDataDate, LocalDate endDataDate) {
        this.workflowId = workflowId;
        this.startDataDate = startDataDate;
        this.endDataDate = endDataDate;
        this.workflowInstanceStatus = WorkflowInstanceStatus.WAITING;
        this.instanceStatus = InstanceStatus.WAITING_DISPATCH;
    }

    public Map toMap() {
        return new HashMap<>();
    }

    public JobComplementVO getFlowComplementVO() {
        JobComplementVO flowComplementVO = new JobComplementVO();
        flowComplementVO.setWorkflowId(this.workflowId);
        flowComplementVO.setStartDataDate(this.startDataDate);
        flowComplementVO.setEndDataDate(this.endDataDate);
        flowComplementVO.setWorkflowInstanceStatus(this.workflowInstanceStatus);
        flowComplementVO.setInstanceStatus(this.instanceStatus);
        return flowComplementVO;
    }
}
