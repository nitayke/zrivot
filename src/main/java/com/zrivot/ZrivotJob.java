package com.zrivot;

import com.zrivot.config.PipelineConfig;
import com.zrivot.pipeline.PipelineOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Entry point for the Zrivot Enrichment Pipeline Flink job.
 *
 * <p>Usage:
 * <pre>
 *   flink run zrivot-enrichment-pipeline.jar [config-path]
 * </pre>
 *
 * <p>If no config path is supplied, the default classpath resource
 * {@code pipeline-config.yaml} is used.</p>
 */
@Slf4j
public class ZrivotJob {
    private static final String DEFAULT_CONFIG_RESOURCE = "pipeline-config.yaml";

    public static void main(String[] args) throws Exception {
        // ── Load configuration ───────────────────────────────────────────
        PipelineConfig config;
        if (args.length > 0) {
            log.info("Loading configuration from file: {}", args[0]);
            config = PipelineConfig.load(args[0]);
        } else {
            log.info("Loading configuration from classpath: {}", DEFAULT_CONFIG_RESOURCE);
            config = PipelineConfig.loadFromClasspath(DEFAULT_CONFIG_RESOURCE);
        }

        log.info("Pipeline mode: {}", config.getMode());
        log.info("Enrichers configured: {}", config.getEnrichers().size());

        // ── Set up Flink environment ─────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(60_000); // 60 seconds

        // ── Build and execute pipeline ───────────────────────────────────
        PipelineOrchestrator orchestrator = new PipelineOrchestrator(config);
        orchestrator.build(env);

        env.execute("Zrivot Enrichment Pipeline [" + config.getMode() + "]");
    }
}
