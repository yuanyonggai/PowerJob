package tech.powerjob.server.core.workflow.complement.vo;

// import com.dhcc.aml.dmp.job.basic.enums.Status;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public final class FlowExecutionVO {

	/**
	 * 流程key
	 */
	private final String flowKey;

	/**
	 * 作业名称
	 */
	private final String flowName;

	/**
	 * 流程执行key
	 */
	private final String flowExecutionKey;

	/**
	 * 数据日期
	 */
	private final String businessDate;

	/**
	 * 流程状态
	 */
	// private Status status;

	/**
	 * 修改时间
	 */
	private LocalDateTime updateTime;

	/**
	 * 开始时间
	 */
	private LocalDateTime startTime;

	/**
	 * 结束时间
	 */
	private LocalDateTime endTime;


	public FlowExecutionVO(String flowKey, String flowName, String flowExecutionKey, String businessDate) {
		this.flowKey = flowKey;
		this.flowName = flowName;
		this.flowExecutionKey = flowExecutionKey;
		this.businessDate = businessDate;
	}

	public FlowExecutionVO getFlowVO() {
		FlowExecutionVO flowExecutionVO = new FlowExecutionVO(this.flowKey, this.flowName, this.flowExecutionKey, this.businessDate);
		// flowExecutionVO.setStatus(this.status);
		flowExecutionVO.setStartTime(this.getStartTime());
		flowExecutionVO.setEndTime(this.getEndTime());
		return flowExecutionVO;
	}

}
