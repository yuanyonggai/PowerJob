package tech.powerjob.server.persistence.remote.model;

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
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "native")
    @GenericGenerator(name = "native", strategy = "native")
    private Long id;
    
    /**
     * 创建日期
     */
    @Column(nullable = false)
    private String createDate;
}
