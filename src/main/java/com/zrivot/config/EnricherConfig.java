package com.zrivot.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a single enricher instance.
 * Each enricher has its own Kafka consumer group, reflow topic, and enrichment implementation.
 */
public class EnricherConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private String consumerGroup;
    private String reflowTopic;
    private String reflowConsumerGroup;
    private String className;
    private Map<String, String> properties = new HashMap<>();

    public EnricherConfig() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getReflowTopic() {
        return reflowTopic;
    }

    public void setReflowTopic(String reflowTopic) {
        this.reflowTopic = reflowTopic;
    }

    public String getReflowConsumerGroup() {
        return reflowConsumerGroup;
    }

    public void setReflowConsumerGroup(String reflowConsumerGroup) {
        this.reflowConsumerGroup = reflowConsumerGroup;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    @Override
    public String toString() {
        return "EnricherConfig{name='" + name + "', consumerGroup='" + consumerGroup +
                "', className='" + className + "'}";
    }
}
