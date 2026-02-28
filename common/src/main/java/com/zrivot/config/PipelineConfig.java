package com.zrivot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;

/**
 * Top-level pipeline configuration.
 *
 * <p>When running inside a Spring Boot application the properties are bound automatically
 * from {@code application.yaml} under the {@code zrivot.*} prefix.  The static
 * {@link #load(String)} and {@link #loadFromClasspath(String)} helpers are kept for
 * standalone / test usage outside the Spring context.</p>
 */
@Data
@ConfigurationProperties(prefix = "zrivot")
public class PipelineConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private PipelineSection pipeline = new PipelineSection();
    private KafkaSection kafka = new KafkaSection();
    private ElasticsearchConfig elasticsearch = new ElasticsearchConfig();
    private JoinerSection joiner = new JoinerSection();
    private ReflowConfig reflow = new ReflowConfig();
    private List<EnricherConfig> enrichers;

    /**
     * Maximum number of concurrent API requests across <b>all</b> enrichers that share
     * the same task manager JVM.  A static semaphore is initialised once on the first
     * operator {@code open()} call.  Default 50.
     */
    private int maxConcurrentApiRequests = 50;

    // ── Loading ──────────────────────────────────────────────────────────

    /**
     * Loads configuration from a YAML file on disk.
     */
    public static PipelineConfig load(String path) throws IOException {
        return YAML_MAPPER.readValue(new File(path), PipelineConfig.class);
    }

    /**
     * Loads configuration from a classpath resource.
     */
    public static PipelineConfig loadFromClasspath(String resource) throws IOException {
        try (InputStream is = PipelineConfig.class.getClassLoader().getResourceAsStream(resource)) {
            if (is == null) {
                throw new IOException("Resource not found on classpath: " + resource);
            }
            return YAML_MAPPER.readValue(is, PipelineConfig.class);
        }
    }

    // ── Convenience accessors ────────────────────────────────────────────

    public PipelineMode getMode() {
        return pipeline.getMode();
    }

    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    public String getRawTopic() {
        return kafka.getRawTopic();
    }

    public String getOutputTopic() {
        return kafka.getOutputTopic();
    }

    public long getJoinerTimeoutMs() {
        return joiner.getTimeoutMs();
    }

    // ── Nested section POJOs ─────────────────────────────────────────────

    @Data
    public static class PipelineSection implements Serializable {
        private static final long serialVersionUID = 1L;
        private PipelineMode mode = PipelineMode.REALTIME;
    }

    @Data
    public static class KafkaSection implements Serializable {
        private static final long serialVersionUID = 1L;
        private String bootstrapServers;
        private String rawTopic;
        private String outputTopic;
    }

    @Data
    public static class JoinerSection implements Serializable {
        private static final long serialVersionUID = 1L;
        private long timeoutMs = 30_000;
    }
}
