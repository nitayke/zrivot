package com.zrivot.clients;

import com.zrivot.ZrivotJobBase;
import com.zrivot.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Entry point for the Clients Enrichment Pipeline Flink job.
 *
 * <p>Usage:
 * <pre>
 *   flink run zrivot-clients.jar [config-path]
 * </pre>
 *
 * <p>If no config path is supplied, the default classpath resource
 * {@code pipeline-config.yaml} is used (clients-specific).</p>
 */
@Slf4j
public class ClientsJob extends ZrivotJobBase {

    private static final String DEFAULT_CONFIG = "pipeline-config.yaml";

    @Override
    protected String getDefaultConfigResource() {
        return DEFAULT_CONFIG;
    }

    @Override
    protected String getJobName(PipelineConfig config) {
        return "Zrivot Clients Pipeline [" + config.getMode() + "]";
    }

    public static void main(String[] args) throws Exception {
        new ClientsJob().run(args);
    }
}
