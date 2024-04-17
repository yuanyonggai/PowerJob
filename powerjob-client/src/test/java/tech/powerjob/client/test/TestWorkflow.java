package tech.powerjob.client.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.ProcessorType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.enums.WorkflowNodeType;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.request.http.SaveWorkflowNodeRequest;
import tech.powerjob.common.request.http.SaveWorkflowRequest;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.response.WorkflowInstanceInfoDTO;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

/**
 * Test cases for {@link PowerJobClient} workflow.
 *
 * @author tjq
 * @author Echo009
 * @since 2020/6/2
 */
class TestWorkflow extends ClientInitializer {

    private static final long WF_ID = 1;

    @Test
    void initTestData() {
        SaveJobInfoRequest base = new SaveJobInfoRequest();
        base.setJobName("DAG-Node-");
        base.setTimeExpressionType(TimeExpressionType.WORKFLOW);
        base.setExecuteType(ExecuteType.STANDALONE);
        base.setProcessorType(ProcessorType.BUILT_IN);
        base.setProcessorInfo("tech.powerjob.samples.workflow.WorkflowStandaloneProcessor");

        for (int i = 0; i < 5; i++) {
            SaveJobInfoRequest request = JSONObject.parseObject(JSONObject.toJSONBytes(base), SaveJobInfoRequest.class);
            request.setJobName(request.getJobName() + i);
            ResultDTO<Long> res = powerJobClient.saveJob(request);
            System.out.println(res);
            Assertions.assertNotNull(res);

        }
    }

    @Test
    void testSaveWorkflow() {

        SaveWorkflowRequest req = new SaveWorkflowRequest();

        req.setWfName("workflow-by-client");
        req.setWfDescription("created by client");
        req.setEnable(true);
        req.setTimeExpressionType(TimeExpressionType.API);

        // System.out.println("req ->" + JSONObject.toJSON(req));
        // ResultDTO<Long> res = powerJobClient.saveWorkflow(req);
        // System.out.println(res);
        // Assertions.assertNotNull(res);

        // req.setId(res.getData());

        // 创建节点
        SaveWorkflowNodeRequest saveWorkflowNodeRequest1 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest1.setJobId(1L);// 特定任务节点id
        saveWorkflowNodeRequest1.setNodeName("DAG-Node-1");
        saveWorkflowNodeRequest1.setNodeParams("nodeparams");// 节点参数
        saveWorkflowNodeRequest1.setType(WorkflowNodeType.JOB.getCode());

        SaveWorkflowNodeRequest saveWorkflowNodeRequest2 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest2.setJobId(1L);
        saveWorkflowNodeRequest2.setNodeName("DAG-Node-2");
        saveWorkflowNodeRequest2.setType(WorkflowNodeType.JOB.getCode());

        SaveWorkflowNodeRequest saveWorkflowNodeRequest3 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest3.setJobId(1L);
        saveWorkflowNodeRequest3.setNodeName("DAG-Node-3");
        saveWorkflowNodeRequest3.setType(WorkflowNodeType.JOB.getCode());

        List<WorkflowNodeInfoDTO> nodeList = powerJobClient.saveWorkflowNode(
                Lists.newArrayList(saveWorkflowNodeRequest1, saveWorkflowNodeRequest3, saveWorkflowNodeRequest2))
                .getData();
        System.out.println(nodeList);
        Assertions.assertNotNull(nodeList);

        // 创建节点
        SaveWorkflowNodeRequest saveWorkflowNodeRequest4 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest4.setJobId(1L);
        saveWorkflowNodeRequest4.setNodeName("DAG-Node-4");
        saveWorkflowNodeRequest4.setType(WorkflowNodeType.JOB.getCode());

        SaveWorkflowNodeRequest saveWorkflowNodeRequest5 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest5.setJobId(1L);
        saveWorkflowNodeRequest5.setNodeName("DAG-Node-5");
        saveWorkflowNodeRequest5.setType(WorkflowNodeType.JOB.getCode());

        SaveWorkflowNodeRequest saveWorkflowNodeRequest6 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest6.setJobId(1L);
        saveWorkflowNodeRequest6.setNodeName("DAG-Node-6");
        saveWorkflowNodeRequest6.setType(WorkflowNodeType.JOB.getCode());

        List<WorkflowNodeInfoDTO> nodeList2 = powerJobClient.saveWorkflowNode(
                Lists.newArrayList(saveWorkflowNodeRequest4, saveWorkflowNodeRequest5, saveWorkflowNodeRequest6))
                .getData();
        System.out.println(nodeList2);
        Assertions.assertNotNull(nodeList2);

        SaveWorkflowNodeRequest saveWorkflowNodeRequest7 = new SaveWorkflowNodeRequest();
        saveWorkflowNodeRequest7.setJobId(1L);
        saveWorkflowNodeRequest7.setNodeName("DAG-Node-7");
        saveWorkflowNodeRequest7.setType(WorkflowNodeType.JOB.getCode());

        List<WorkflowNodeInfoDTO> nodeList3 = powerJobClient
                .saveWorkflowNode(Lists.newArrayList(saveWorkflowNodeRequest7)).getData();
        System.out.println(nodeList3);
        Assertions.assertNotNull(nodeList3);

        // DAG 图
        List<PEWorkflowDAG.Node> nodes = Lists.newLinkedList();
        List<PEWorkflowDAG.Edge> edges = Lists.newLinkedList();

        nodes.add(new PEWorkflowDAG.Node(nodeList.get(0).getId()));
        nodes.add(new PEWorkflowDAG.Node(nodeList.get(1).getId()));
        nodes.add(new PEWorkflowDAG.Node(nodeList.get(2).getId()));

        edges.add(new PEWorkflowDAG.Edge(nodeList.get(0).getId(), nodeList.get(1).getId()));
        edges.add(new PEWorkflowDAG.Edge(nodeList.get(1).getId(), nodeList.get(2).getId()));

        nodes.add(new PEWorkflowDAG.Node(nodeList2.get(0).getId()));
        nodes.add(new PEWorkflowDAG.Node(nodeList2.get(1).getId()));
        nodes.add(new PEWorkflowDAG.Node(nodeList2.get(2).getId()));

        nodes.add(new PEWorkflowDAG.Node(nodeList3.get(0).getId()));

        edges.add(new PEWorkflowDAG.Edge(nodeList2.get(0).getId(), nodeList2.get(1).getId()));
        edges.add(new PEWorkflowDAG.Edge(nodeList2.get(1).getId(), nodeList2.get(2).getId()));

        edges.add(new PEWorkflowDAG.Edge(nodeList.get(2).getId(), nodeList3.get(0).getId()));
        edges.add(new PEWorkflowDAG.Edge(nodeList2.get(2).getId(), nodeList3.get(0).getId()));

        PEWorkflowDAG peWorkflowDAG = new PEWorkflowDAG(nodes, edges);

        // 保存完整信息
        req.setDag(peWorkflowDAG);
        ResultDTO<Long> res = powerJobClient.saveWorkflow(req);

        System.out.println(res);
        Assertions.assertNotNull(res);

    }

    public static void main(String[] args) {
        int numTasks = 10; // 总任务数量
        int numGroups = 3; // 分组数量

        if (numTasks < numGroups) {
            throw new RuntimeException("并发数不合理，总任务数比并发数小");
        }

        int tasksPerGroup = numTasks / numGroups; // 每组的基本任务数量
        int extraTasks = numTasks % numGroups; // 额外分配的任务数量

        List<List<Integer>> taskGroups = new ArrayList<>();
        int taskId = 1;

        for (int i = 0; i < numGroups; i++) {
            int groupSize = tasksPerGroup;
            if (i < extraTasks) {
                groupSize++; // 如果有余数，则给前面的组多分配一个任务
            }

            List<Integer> group = new ArrayList<>();
            for (int j = 0; j < groupSize; j++) {
                group.add(taskId);
                taskId++;
            }

            taskGroups.add(group);
        }

        for (List<Integer> group : taskGroups) {
            System.out.print("Group: ");
            for (int taskIdd : group) {
                System.out.print(taskIdd + " ");
            }
            System.out.println();
        }

    }

    @Test
    /**
     * 测试500个节点，并行5，效果如何？
     */
    void testSaveWorkflow_500() {

        SaveWorkflowRequest req = new SaveWorkflowRequest();

        req.setWfName("workflow-by-client 500");
        req.setWfDescription("created by client 500");
        req.setEnable(true);
        req.setTimeExpressionType(TimeExpressionType.API);

        // 创建节点 500个
        List<SaveWorkflowNodeRequest> requestNodeList = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            SaveWorkflowNodeRequest saveWorkflowNodeRequest = new SaveWorkflowNodeRequest();
            saveWorkflowNodeRequest.setJobId(1L);
            saveWorkflowNodeRequest.setNodeName("DAG-Node-" + i);
            saveWorkflowNodeRequest.setType(WorkflowNodeType.JOB.getCode());
            requestNodeList.add(saveWorkflowNodeRequest);
        }
        List<WorkflowNodeInfoDTO> nodeList = powerJobClient.saveWorkflowNode(requestNodeList).getData();
        System.out.println(nodeList.size());
        Assertions.assertNotNull(nodeList);

        // DAG 图
        List<PEWorkflowDAG.Node> nodes = Lists.newLinkedList();
        List<PEWorkflowDAG.Edge> edges = Lists.newLinkedList();

        // node
        for (int i = 0; i < 500; i++) {
            nodes.add(new PEWorkflowDAG.Node(nodeList.get(i).getId()));
        }

        // edge
        for (int i = 0; i < 100; i++) {
        edges.add(new PEWorkflowDAG.Edge(nodeList.get(i).getId(),
        nodeList.get(i+1).getId()));
        }

        for (int i = 101; i < 200; i++) {
        edges.add(new PEWorkflowDAG.Edge(nodeList.get(i).getId(),
        nodeList.get(i+1).getId()));
        }

        for (int i = 201; i < 300; i++) {
        edges.add(new PEWorkflowDAG.Edge(nodeList.get(i).getId(),
        nodeList.get(i+1).getId()));
        }

        for (int i = 301; i < 400; i++) {
        edges.add(new PEWorkflowDAG.Edge(nodeList.get(i).getId(),
        nodeList.get(i+1).getId()));
        }

        for (int i = 401; i < 499; i++) {
        edges.add(new PEWorkflowDAG.Edge(nodeList.get(i).getId(),
        nodeList.get(i+1).getId()));
        }

        PEWorkflowDAG peWorkflowDAG = new PEWorkflowDAG(nodes, edges);

        // 保存完整信息
        req.setDag(peWorkflowDAG);
        ResultDTO<Long> res = powerJobClient.saveWorkflow(req);

        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testCopyWorkflow() {
        ResultDTO<Long> res = powerJobClient.copyWorkflow(WF_ID);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testDisableWorkflow() {
        ResultDTO<Void> res = powerJobClient.disableWorkflow(WF_ID);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testDeleteWorkflow() {
        ResultDTO<Void> res = powerJobClient.deleteWorkflow(20L);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testEnableWorkflow() {
        ResultDTO<Void> res = powerJobClient.enableWorkflow(WF_ID);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testFetchWorkflowInfo() {
        ResultDTO<WorkflowInfoDTO> res = powerJobClient.fetchWorkflow(WF_ID);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testRunWorkflow() {
        ResultDTO<Long> res = powerJobClient.runWorkflow(WF_ID, null, 0);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testStopWorkflowInstance() {
        ResultDTO<Void> res = powerJobClient.stopWorkflowInstance(149962433421639744L);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testRetryWorkflowInstance() {
        ResultDTO<Void> res = powerJobClient.retryWorkflowInstance(149962433421639744L);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testMarkWorkflowNodeAsSuccess() {
        ResultDTO<Void> res = powerJobClient.markWorkflowNodeAsSuccess(149962433421639744L, 1L);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testFetchWfInstanceInfo() {
        ResultDTO<WorkflowInstanceInfoDTO> res = powerJobClient.fetchWorkflowInstanceInfo(149962433421639744L);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }

    @Test
    void testRunWorkflowPlus() {
        ResultDTO<Long> res = powerJobClient.runWorkflow(WF_ID, "this is init Params 2", 90000);
        System.out.println(res);
        Assertions.assertNotNull(res);
    }
}
