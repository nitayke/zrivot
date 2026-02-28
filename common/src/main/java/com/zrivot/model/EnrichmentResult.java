package com.zrivot.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Result produced by a single enricher for a specific document.
 * Contains either enriched fields (on success) or error details (on failure).
 *
 * <p>The {@code originalPayload} is the raw document payload carried as a
 * {@code Map<String, Object>} through the pipeline.</p>
 */
@Data
@NoArgsConstructor
public class EnrichmentResult implements Serializable {

    private static final long serialVersionUID = 2L;

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
}
