package tech.powerjob.server.persistence.remote.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 工作流队列信息
 * 记录了外部消息队列发送的调度任务信息
 *
 * @author yyg
 * @since 2024/4/18
 */
@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(indexes = {
        @Index(name = "idx01_workflow_queue_info", columnList = "gdhStatus,gmtCreate")
})
public class WorkflowQueueInfoDO {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "native")
    @GenericGenerator(name = "native", strategy = "native")
    private Long id;

    /**
     * 工单号
     */
    @Column(nullable = false)
    private String gdh;

    /**
     * 工单号状态(WAITING(1, "等待调度"), RUNNING(2, "运行中"), FAILED(3, "失败"), SUCCEED(4, "成功"), STOPPED(10, "手动停止");)
     */
    @Column(nullable = false)
    private Integer gdhStatus;

    /**
     * 工单类型
     */
    @Column(nullable = false)
    private String gdhType;

    /**
     * 工作流id
     */
    @Column(nullable = false)
    private Long workflowId;   

    /**
     * 工作流实例id
     */
    private Long wfInstanceId;
    
    /**
     * 开始时间
     */
    private Date startTime;

    /**
     * 结束时间
     */
    private Date endTime;

    /**
     * 执行信息
     */
    private String runDesc;

    /**
     * 流程运行数据对象
     */
    private String flowRunDto;
    
    /**
     * 创建时间
     */
    @Column(nullable = false)
    private Date gmtCreate;

    /**
     * 更新时间
     */
    @Column(nullable = false)
    private Date gmtModified;
}
