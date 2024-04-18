package tech.powerjob.server.persistence.remote.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import tech.powerjob.server.persistence.remote.model.WorkflowQueueInfoDO;

/**
 * WorkflowQueueInfo 数据访问层
 *
 * @author yyg
 * @since 2024/4/18
 */
public interface WorkflowQueueInfoRepository extends JpaRepository<WorkflowQueueInfoDO, Long> {
    /**
     * 根据工单号状态查找
     *
     * @param gdhStatus 工单号状态
     * @return 工作流队列信息集合
     */
    List<WorkflowQueueInfoDO> findByGdhStatus(Integer gdhStatus, Pageable pageable); 
}
