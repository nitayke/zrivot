package com.zrivot;

import com.zrivot.config.PipelineConfig;
import com.zrivot.pipeline.PipelineOrchestrator;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ZrivotJob {

    private static final Logger LOG = LoggerFactory.getLogger(ZrivotJob.class);
    private static final String DEFAULT_CONFIG_RESOURCE = "pipeline-config.yaml";

    public static void main(String[] args) throws Exception {
        // ── Load configuration ───────────────────────────────────────────
        PipelineConfig config;
        if (args.length > 0) {
            LOG.info("Loading configuration from file: {}", args[0]);
            config = PipelineConfig.load(args[0]);
        } else {
            LOG.info("Loading configuration from classpath: {}", DEFAULT_CONFIG_RESOURCE);
            config = PipelineConfig.loadFromClasspath(DEFAULT_CONFIG_RESOURCE);
        }

        LOG.info("Pipeline mode: {}", config.getMode());
        LOG.info("Enrichers configured: {}", config.getEnrichers().size());

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
