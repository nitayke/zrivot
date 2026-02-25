package com.zrivot.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Result produced by a single enricher for a specific document.
 * Contains either enriched fields (on success) or error details (on failure).
 */
public class EnrichmentResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String documentId;
    private String enricherName;
    private Map<String, Object> enrichedFields;
    private Map<String, Object> originalPayload;
    private long boomerangUpdateCount;
    private boolean success;
    private String errorMessage;
    private boolean reflow;
    private Map<String, Map<String, Object>> existingEnrichments;
    private long timestamp;

    public EnrichmentResult() {
    }

    public static EnrichmentResult success(String documentId, String enricherName,
                                           Map<String, Object> enrichedFields,
                                           Map<String, Object> originalPayload,
                                           long boomerangUpdateCount,
                                           boolean reflow,
                                           Map<String, Map<String, Object>> existingEnrichments) {
        EnrichmentResult result = new EnrichmentResult();
        result.documentId = documentId;
        result.enricherName = enricherName;
        result.enrichedFields = enrichedFields;
        result.originalPayload = originalPayload;
        result.boomerangUpdateCount = boomerangUpdateCount;
        result.success = true;
        result.reflow = reflow;
        result.existingEnrichments = existingEnrichments;
        result.timestamp = System.currentTimeMillis();
        return result;
    }

    public static EnrichmentResult failure(String documentId, String enricherName,
                                           String errorMessage,
                                           Map<String, Object> originalPayload,
                                           long boomerangUpdateCount,
                                           boolean reflow,
                                           Map<String, Map<String, Object>> existingEnrichments) {
        EnrichmentResult result = new EnrichmentResult();
        result.documentId = documentId;
        result.enricherName = enricherName;
        result.errorMessage = errorMessage;
        result.originalPayload = originalPayload;
        result.boomerangUpdateCount = boomerangUpdateCount;
        result.success = false;
        result.reflow = reflow;
        result.existingEnrichments = existingEnrichments;
        result.timestamp = System.currentTimeMillis();
        return result;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public String getEnricherName() {
        return enricherName;
    }

    public void setEnricherName(String enricherName) {
        this.enricherName = enricherName;
    }

    public Map<String, Object> getEnrichedFields() {
        return enrichedFields;
    }

    public void setEnrichedFields(Map<String, Object> enrichedFields) {
        this.enrichedFields = enrichedFields;
    }

    public Map<String, Object> getOriginalPayload() {
        return originalPayload;
    }

    public void setOriginalPayload(Map<String, Object> originalPayload) {
        this.originalPayload = originalPayload;
    }

    public long getBoomerangUpdateCount() {
        return boomerangUpdateCount;
    }

    public void setBoomerangUpdateCount(long boomerangUpdateCount) {
        this.boomerangUpdateCount = boomerangUpdateCount;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "EnrichmentResult{documentId='" + documentId + "', enricherName='" + enricherName +
                "', success=" + success + ", reflow=" + reflow + "}";
    }
}
