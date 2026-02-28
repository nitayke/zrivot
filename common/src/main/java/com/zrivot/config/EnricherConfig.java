package com.zrivot.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a single enricher instance.
 * Each enricher has its own Kafka consumer group, reflow topic, and enrichment implementation.
 */
@Data
@NoArgsConstructor
public class EnricherConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    private String name;
    private String consumerGroup;
    private String reflowTopic;
    private String reflowConsumerGroup;
    private String className;
    private Map<String, String> properties = new HashMap<>();

    /**
     * Whether this enricher participates in reflow (ES re-enrichment).
     * When {@code false}, no reflow pipeline is built for this enricher and the
     * boomerang update-count guard is skipped in the realtime path.
     */
    private boolean reflowEnabled = true;

    /** Timeout for Flink's AsyncDataStream per-element wait (default: 30 seconds). */
    private long asyncTimeoutMs = 30_000;

    /** Max number of in-flight async enrichment calls per operator (default: 100). */
    private int asyncCapacity = 100;

    /** Whether this enricher accepts bulk (batch) API requests. */
    private boolean bulkEnabled = false;

    /** Number of documents per bulk request (used when {@code bulkEnabled} is true). */
    private int bulkSize = 10;

    /** Delay in ms before retrying a failed enrichment (reflow only, re-fetches from ES). */
    private long retryDelayMs = 5_000;

    /** Maximum number of retries after enrichment failure (reflow only). */
    private int maxRetries = 3;

    public String getProperty(String key) {
        return properties.get(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }
}
