package com.zrivot.clients;

import com.zrivot.ZrivotJobBase;
import com.zrivot.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Clients Enrichment Pipeline Flink job.
 *
 * <p>Usage:
 * <pre>
 *   flink run zrivot-clients.jar
 * </pre>
 *
 * <p>Configuration is loaded automatically by Spring Boot from
 * {@code application.yaml} on the classpath.</p>
 */
@Slf4j
@SpringBootApplication(scanBasePackages = "com.zrivot")
public class ClientsJob extends ZrivotJobBase {

    @Override
    protected String getJobName(PipelineConfig config) {
        return "Zrivot Clients Pipeline [" + config.getMode() + "]";
    }

    public static void main(String[] args) {
        SpringApplication.run(ClientsJob.class, args);
    }
}
