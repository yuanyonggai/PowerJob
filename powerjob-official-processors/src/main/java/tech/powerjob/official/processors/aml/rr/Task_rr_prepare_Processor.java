package tech.powerjob.official.processors.aml.rr;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.official.processors.CommonBasicProcessor;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.log.OmsLogger;

/**
 * 监管报送rr(Regulatory reporting)--数据准备跑批任务处理器
 * 
 * @author yyg
 * @since 2024/4/11
 */
@Component("rr_prepare")
@Slf4j
public class Task_rr_prepare_Processor extends CommonBasicProcessor {

    @Override
    protected ProcessResult process0(TaskContext taskContext) throws Exception {

        OmsLogger omsLogger = taskContext.getOmsLogger();
        String message = "监管报送rr(Regulatory reporting)--数据准备跑批任务处理器 运行成功！";
        omsLogger.warn(message);

        return new ProcessResult(true, "execute successfully");
    }

}
