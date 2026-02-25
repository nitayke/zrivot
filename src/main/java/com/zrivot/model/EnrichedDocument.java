package com.zrivot.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Fully enriched document ready for output to Kafka.
 * Contains the original payload merged with all enrichment results.
 */
public class EnrichedDocument implements Serializable {

    private static final long serialVersionUID = 1L;

    private String documentId;
    private Map<String, Object> originalPayload;
    private Map<String, Map<String, Object>> enrichments;
    private long timestamp;

    public EnrichedDocument() {
    }

    public EnrichedDocument(String documentId,
                            Map<String, Object> originalPayload,
                            Map<String, Map<String, Object>> enrichments,
                            long timestamp) {
        this.documentId = documentId;
        this.originalPayload = originalPayload;
        this.enrichments = enrichments;
        this.timestamp = timestamp;
    }

    /**
     * Produces a flat merged view: original payload + all enrichments
     * under their respective enricher-name keys.
     */
    public Map<String, Object> toMergedMap() {
        Map<String, Object> merged = new HashMap<>();
        if (originalPayload != null) {
            merged.putAll(originalPayload);
        }
        merged.put("documentId", documentId);
        merged.put("timestamp", timestamp);
        if (enrichments != null) {
            merged.put("enrichments", enrichments);
        }
        return merged;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public Map<String, Object> getOriginalPayload() {
        return originalPayload;
    }

    public void setOriginalPayload(Map<String, Object> originalPayload) {
        this.originalPayload = originalPayload;
    }

    public Map<String, Map<String, Object>> getEnrichments() {
        return enrichments;
    }

    public void setEnrichments(Map<String, Map<String, Object>> enrichments) {
        this.enrichments = enrichments;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        int enrichmentCount = enrichments != null ? enrichments.size() : 0;
        return "EnrichedDocument{documentId='" + documentId +
                "', enrichmentCount=" + enrichmentCount + "}";
    }
}
