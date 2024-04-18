package tech.powerjob.server.persistence.remote.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import tech.powerjob.server.persistence.remote.model.WorkflowAllRunInfoDO;

/**
 * WorkflowAllRunInfo 数据访问层
 *
 * @author yyg
 * @since 2024/4/18
 */
public interface WorkflowAllRunInfoRepository extends JpaRepository<WorkflowAllRunInfoDO, Long> {
    /**
     * 根据日期查询有多少条记录
     *
     * @param gdhStatus 工单号状态
     * @return 工作流队列信息集合
     */
    long countByCreateDate(String createDate);
}
