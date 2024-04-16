package tech.powerjob.official.processors.impl.java;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.official.processors.CommonBasicProcessor;
import tech.powerjob.official.processors.impl.java.executable.ExecutableResourceFactory;
import tech.powerjob.official.processors.util.CommonUtils;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import tech.powerjob.worker.log.OmsLogger;

/**
 * common java processor
 *
 * @author yyg
 * @since 2024/4/11
 */
@Component("JavaProcessor")
@Slf4j
public class JavaProcessor extends CommonBasicProcessor {

    @Override
    public ProcessResult process0(TaskContext taskContext) throws Exception {
        
        Map<String, String> workflowContext = taskContext.getWorkflowContext().fetchWorkflowContext();
        log.info("工作流上下文数据:{}", workflowContext);
        log.info("用户上下文数据:{}", taskContext.getUserContext());

        OmsLogger omsLogger = taskContext.getOmsLogger();
        String javaScriptParams = CommonUtils.parseParams(taskContext);

        if (StringUtils.isBlank(javaScriptParams)) {
            String message = "[SYSTEM] javaScriptParams is null, please check jobParam configuration.";
            omsLogger.warn(message);
            return new ProcessResult(false, message);
        }

        try {
            BasicProcessor javaProcessor = ExecutableResourceFactory.getInstance().loadNewInstance(javaScriptParams);
            javaProcessor.process(taskContext);
        } catch (Exception e) {
            return new ProcessResult(false, "execute error");
        }

        return new ProcessResult(true, "execute successfully");
    }

}
