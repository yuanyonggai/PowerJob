package tech.powerjob.samples.tester;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tech.powerjob.worker.annotation.PowerJobHandler;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import tech.powerjob.worker.log.OmsLogger;

@Slf4j
@Component("YYGtestStandaloneProcessorDemo")
public class YYGtestStandaloneProcessorDemo implements BasicProcessor {

    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        // PowerJob 在线日志功能，使用该 Logger 打印的日志可以直接在 PowerJob 控制台查看
        OmsLogger omsLogger = context.getOmsLogger();
        omsLogger.info("yyg test StandaloneProcessorDemo start process,context is {}.", context);

        return new ProcessResult(true, "process successfully~");
    }

    @PowerJobHandler(name = "testEmptyReturn")
    public ProcessResult testMethod(TaskContext context){
        return new ProcessResult(true, "process successfully~");
    }

    @PowerJobHandler(name = "testMethodSleep")
    public ProcessResult testMethodSleep(TaskContext context) throws InterruptedException {
        OmsLogger omsLogger = context.getOmsLogger();
        omsLogger.info("yyg test StandaloneProcessorDemo start process,context is {}.", context);
        context.getWorkflowContext().appendData2WfContext("testC", 1);
        try{
            System.out.println("开始休眠...");
            Thread.sleep(1);//300000毫秒就是5分钟
            System.out.println("休眠结束.");
        }catch(InterruptedException e){
//线程中断时的处理
            e.printStackTrace();
        }
        return new ProcessResult(true, "process successfully~");
    }

}