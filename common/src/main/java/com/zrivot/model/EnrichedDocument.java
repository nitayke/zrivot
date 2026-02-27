package com.zrivot.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Fully enriched document ready for output to Kafka.
 * Contains the original payload merged with all enrichment results.
 *
 * <p>The {@code originalPayload} is the typed domain object (e.g. PurchaseDocument)
 * carried as {@code Object}.  It is converted to a map at serialization time
 * when building the merged output.</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedDocument implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String documentId;
    private Object originalPayload;
    private Map<String, Map<String, Object>> enrichments;
    private long timestamp;

    /**
     * Produces a flat merged view: original payload + all enrichments
     * under their respective enricher-name keys.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> toMergedMap() {
        Map<String, Object> merged = new HashMap<>();
        if (originalPayload != null) {
            if (originalPayload instanceof Map) {
                merged.putAll((Map<String, Object>) originalPayload);
            } else {
                merged.putAll(MAPPER.convertValue(originalPayload,
                        new TypeReference<Map<String, Object>>() {}));
            }
        }
        merged.put("documentId", documentId);
        merged.put("timestamp", timestamp);
        if (enrichments != null) {
            merged.put("enrichments", enrichments);
        }
        return merged;
    }
}
