package com.zrivot;

import com.zrivot.config.PipelineConfig;
import com.zrivot.pipeline.PipelineOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

/**
 * Abstract base for domain-specific Flink jobs, wired via Spring Boot.
 *
 * <p>Subclasses are {@code @SpringBootApplication} entry points that only need to
 * provide the Flink job display name.  Configuration is loaded automatically by
 * Spring Boot from {@code application.yaml} and injected via
 * {@link com.zrivot.config.ZrivotPipelineAutoConfiguration}.</p>
 *
 * <p>Usage in a sub-project:
 * <pre>
 *   {@literal @}SpringBootApplication(scanBasePackages = "com.zrivot")
 *   public class PurchasesJob extends ZrivotJobBase {
 *       protected String getJobName(PipelineConfig c) { return "Purchases [" + c.getMode() + "]"; }
 *       public static void main(String[] args) { SpringApplication.run(PurchasesJob.class, args); }
 *   }
 * </pre>
 */
@Slf4j
public abstract class ZrivotJobBase implements CommandLineRunner {

    @Autowired
    private PipelineConfig config;

    @Autowired
    private PipelineOrchestrator orchestrator;

    /**
     * Display name shown in the Flink dashboard.
     */
    protected abstract String getJobName(PipelineConfig config);

    @Override
    public void run(String... args) throws Exception {
        log.info("Pipeline mode: {}", config.getMode());
        log.info("Enrichers configured: {}", config.getEnrichers().size());

        // ── Set up Flink environment ─────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(60_000);

        // ── Build and execute pipeline ───────────────────────────────────
        orchestrator.build(env);

        env.execute(getJobName(config));
    }
}
