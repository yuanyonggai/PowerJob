package tech.powerjob.server.persistence.remote.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 完整跑批工作流跑批记录
 *
 * @author yyg
 * @since 2024/4/18
 */
@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(indexes = {
        @Index(name = "idx01_workflow_all_run_info", columnList = "createDate")
})
public class WorkflowAllRunInfoDO {
    /**
     * 创建日期
     */
    @Column(nullable = false)
    private String createDate;
}
