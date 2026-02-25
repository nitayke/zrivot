package com.zrivot.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a raw document entering the enrichment pipeline.
 * Can originate from the Kafka raw topic (realtime) or from Elasticsearch (reflow).
 */
public class RawDocument implements Serializable {

    private static final long serialVersionUID = 1L;

    private String documentId;
    private Map<String, Object> payload;
    private long boomerangUpdateCount;
    private long timestamp;
    private boolean reflow;
    private Map<String, Map<String, Object>> existingEnrichments;

    public RawDocument() {
    }

    private RawDocument(Builder builder) {
        this.documentId = builder.documentId;
        this.payload = builder.payload;
        this.boomerangUpdateCount = builder.boomerangUpdateCount;
        this.timestamp = builder.timestamp;
        this.reflow = builder.reflow;
        this.existingEnrichments = builder.existingEnrichments;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public long getBoomerangUpdateCount() {
        return boomerangUpdateCount;
    }

    public void setBoomerangUpdateCount(long boomerangUpdateCount) {
        this.boomerangUpdateCount = boomerangUpdateCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isReflow() {
        return reflow;
    }

    public void setReflow(boolean reflow) {
        this.reflow = reflow;
    }

    public Map<String, Map<String, Object>> getExistingEnrichments() {
        return existingEnrichments;
    }

    public void setExistingEnrichments(Map<String, Map<String, Object>> existingEnrichments) {
        this.existingEnrichments = existingEnrichments;
    }

    @Override
    public String toString() {
        return "RawDocument{documentId='" + documentId + "', boomerangUpdateCount=" +
                boomerangUpdateCount + ", reflow=" + reflow + "}";
    }

    public static final class Builder {
        private String documentId;
        private Map<String, Object> payload = new HashMap<>();
        private long boomerangUpdateCount;
        private long timestamp = System.currentTimeMillis();
        private boolean reflow;
        private Map<String, Map<String, Object>> existingEnrichments;

        private Builder() {
        }

        public Builder documentId(String documentId) {
            this.documentId = documentId;
            return this;
        }

        public Builder payload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }

        public Builder boomerangUpdateCount(long boomerangUpdateCount) {
            this.boomerangUpdateCount = boomerangUpdateCount;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder reflow(boolean reflow) {
            this.reflow = reflow;
            return this;
        }

        public Builder existingEnrichments(Map<String, Map<String, Object>> existingEnrichments) {
            this.existingEnrichments = existingEnrichments;
            return this;
        }

        public RawDocument build() {
            return new RawDocument(this);
        }
    }
}
