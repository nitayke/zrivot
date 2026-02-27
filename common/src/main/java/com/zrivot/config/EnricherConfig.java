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

    /** Timeout for Flink's AsyncDataStream per-element wait (default: 30 seconds). */
    private long asyncTimeoutMs = 30_000;

    /** Max number of in-flight async enrichment calls per operator (default: 100). */
    private int asyncCapacity = 100;

    public String getProperty(String key) {
        return properties.get(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }
}
