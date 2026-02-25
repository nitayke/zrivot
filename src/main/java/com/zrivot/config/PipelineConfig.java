package com.zrivot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;

/**
 * Top-level pipeline configuration. Loaded from a YAML file at job startup.
 *
 * <p>Encapsulates all settings for: pipeline mode, Kafka connectivity,
 * Elasticsearch connectivity, enricher definitions, joiner behaviour, and reflow settings.</p>
 */
public class PipelineConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private PipelineSection pipeline = new PipelineSection();
    private KafkaSection kafka = new KafkaSection();
    private ElasticsearchConfig elasticsearch = new ElasticsearchConfig();
    private JoinerSection joiner = new JoinerSection();
    private ReflowConfig reflow = new ReflowConfig();
    private List<EnricherConfig> enrichers;

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

    // ── Getters / Setters ────────────────────────────────────────────────

    public PipelineSection getPipeline() {
        return pipeline;
    }

    public void setPipeline(PipelineSection pipeline) {
        this.pipeline = pipeline;
    }

    public KafkaSection getKafka() {
        return kafka;
    }

    public void setKafka(KafkaSection kafka) {
        this.kafka = kafka;
    }

    public ElasticsearchConfig getElasticsearch() {
        return elasticsearch;
    }

    public void setElasticsearch(ElasticsearchConfig elasticsearch) {
        this.elasticsearch = elasticsearch;
    }

    public JoinerSection getJoiner() {
        return joiner;
    }

    public void setJoiner(JoinerSection joiner) {
        this.joiner = joiner;
    }

    public ReflowConfig getReflow() {
        return reflow;
    }

    public void setReflow(ReflowConfig reflow) {
        this.reflow = reflow;
    }

    public List<EnricherConfig> getEnrichers() {
        return enrichers;
    }

    public void setEnrichers(List<EnricherConfig> enrichers) {
        this.enrichers = enrichers;
    }

    // ── Nested section POJOs ─────────────────────────────────────────────

    public static class PipelineSection implements Serializable {
        private static final long serialVersionUID = 1L;
        private PipelineMode mode = PipelineMode.REALTIME;

        public PipelineMode getMode() {
            return mode;
        }

        public void setMode(PipelineMode mode) {
            this.mode = mode;
        }
    }

    public static class KafkaSection implements Serializable {
        private static final long serialVersionUID = 1L;
        private String bootstrapServers;
        private String rawTopic;
        private String outputTopic;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getRawTopic() {
            return rawTopic;
        }

        public void setRawTopic(String rawTopic) {
            this.rawTopic = rawTopic;
        }

        public String getOutputTopic() {
            return outputTopic;
        }

        public void setOutputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
        }
    }

    public static class JoinerSection implements Serializable {
        private static final long serialVersionUID = 1L;
        private long timeoutMs = 30_000;

        public long getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }
    }
}
