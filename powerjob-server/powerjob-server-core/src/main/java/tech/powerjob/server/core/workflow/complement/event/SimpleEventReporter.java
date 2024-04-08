package tech.powerjob.server.core.workflow.complement.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class SimpleEventReporter implements EventReporter {

    @Autowired
    AsyncEventReport asyncEventReport;

    @Override
    public boolean report(EventType eventType, Map<String, String> metadata) {
        //待扩展，用于对外消息发布，如：redis,zookeeper,消息中间件，监控平台等
        asyncEventReport.asyncReport(eventType, metadata);

        return true;
    }

}
