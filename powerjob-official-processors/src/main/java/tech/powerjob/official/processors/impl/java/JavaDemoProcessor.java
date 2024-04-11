package tech.powerjob.official.processors.impl.java;

import org.springframework.stereotype.Component;

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
@Component("JavaDemoProcessor")
public class JavaDemoProcessor implements BasicProcessor {

    @Override
    public ProcessResult process(TaskContext taskContext) throws Exception {
        OmsLogger omsLogger = taskContext.getOmsLogger();
        int i = 55;
        omsLogger.info("i:{}", i + 1);

        String message = "execute successfully";
        return new ProcessResult(true, message);
    }

}
