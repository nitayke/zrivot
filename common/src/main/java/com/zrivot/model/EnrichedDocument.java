package com.zrivot.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Fully enriched document ready for output to Kafka.
 * Contains the original payload merged with all enrichment results.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedDocument implements Serializable {

    private static final long serialVersionUID = 3L;

    private String documentId;
    private Map<String, Object> originalPayload;
    private Map<String, Map<String, Object>> enrichments;
    private long timestamp;

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
}
