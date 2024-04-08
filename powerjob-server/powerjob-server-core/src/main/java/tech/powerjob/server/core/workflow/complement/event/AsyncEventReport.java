package tech.powerjob.server.core.workflow.complement.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class AsyncEventReport {

    @Async
    public void asyncReport(EventType eventType, Map<String, String> metadata) {
        log.info(".......AsyncEventReport....." + eventType + ":" + metadata);

    }
}
