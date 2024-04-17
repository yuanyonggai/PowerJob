package tech.powerjob.samples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 主类
 *
 * @author tjq
 * @since 2020/4/17
 */
@EnableScheduling
@SpringBootApplication(scanBasePackages = { "tech.powerjob.official.processors.impl",
        "tech.powerjob.official.processors.impl.java", "tech.powerjob.samples.config",
        "tech.powerjob.official.processors.impl.script", "tech.powerjob.official.processors.impl.sql", "tech.powerjob.official.processors.aml.rr" })
public class SampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(SampleApplication.class, args);
    }
}
