package com.zrivot.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Message from a reflow Kafka topic indicating "what changed" for a specific enricher.
 * The query criteria are translated into an Elasticsearch query to find affected documents.
 */
public class ReflowMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String enricherName;
    private Map<String, Object> queryCriteria;
    private long timestamp;

    public ReflowMessage() {
    }

    public ReflowMessage(String enricherName, Map<String, Object> queryCriteria, long timestamp) {
        this.enricherName = enricherName;
        this.queryCriteria = queryCriteria;
        this.timestamp = timestamp;
    }

    public String getEnricherName() {
        return enricherName;
    }

    public void setEnricherName(String enricherName) {
        this.enricherName = enricherName;
    }

    public Map<String, Object> getQueryCriteria() {
        return queryCriteria;
    }

    public void setQueryCriteria(Map<String, Object> queryCriteria) {
        this.queryCriteria = queryCriteria;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReflowMessage{enricherName='" + enricherName +
                "', queryCriteria=" + queryCriteria + "}";
    }
}
