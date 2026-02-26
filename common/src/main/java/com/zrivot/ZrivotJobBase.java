package com.zrivot;

import com.zrivot.config.PipelineConfig;
import com.zrivot.pipeline.PipelineOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Abstract base for domain-specific Flink jobs.
 *
 * <p>Subclasses only need to provide the default config resource name and
 * the Flink job display name. The pipeline is fully generic and is driven
 * by the YAML configuration.</p>
 *
 * <p>Usage in a sub-project:
 * <pre>
 *   public class PurchasesJob extends ZrivotJobBase {
 *       protected String getDefaultConfigResource() { return "pipeline-config.yaml"; }
 *       protected String getJobName(PipelineConfig c) { return "Purchases [" + c.getMode() + "]"; }
 *       public static void main(String[] args) throws Exception { new PurchasesJob().run(args); }
 *   }
 * </pre>
 */
@Slf4j
public abstract class ZrivotJobBase {

    /**
     * Classpath resource loaded when no command-line config path is supplied.
     */
    protected abstract String getDefaultConfigResource();

    /**
     * Display name shown in the Flink dashboard.
     */
    protected abstract String getJobName(PipelineConfig config);

    /**
     * Runs the enrichment pipeline end-to-end.
     *
     * @param args optional single argument: path to a YAML config file
     */
    public void run(String[] args) throws Exception {
        // ── Load configuration ───────────────────────────────────────────
        PipelineConfig config;
        if (args.length > 0) {
            log.info("Loading configuration from file: {}", args[0]);
            config = PipelineConfig.load(args[0]);
        } else {
            String resource = getDefaultConfigResource();
            log.info("Loading configuration from classpath: {}", resource);
            config = PipelineConfig.loadFromClasspath(resource);
        }

        log.info("Pipeline mode: {}", config.getMode());
        log.info("Enrichers configured: {}", config.getEnrichers().size());

        // ── Set up Flink environment ─────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(60_000);

        // ── Build and execute pipeline ───────────────────────────────────
        PipelineOrchestrator orchestrator = new PipelineOrchestrator(config);
        orchestrator.build(env);

        env.execute(getJobName(config));
    }
}
