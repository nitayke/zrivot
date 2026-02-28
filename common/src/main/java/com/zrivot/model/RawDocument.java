package com.zrivot.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a raw document entering the enrichment pipeline.
 * Can originate from the Kafka raw topic (realtime) or from Elasticsearch (reflow).
 *
 * <p>The payload is carried as a generic {@code Map<String, Object>} through the pipeline.
 * Per-enricher mappers ({@link com.zrivot.enrichment.Enricher#mapToRequest}) convert
 * the map to/from typed domain objects as needed for API calls.</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawDocument implements Serializable {

    private static final long serialVersionUID = 3L;

    private String documentId;
    @Builder.Default
    private Map<String, Object> payload = new HashMap<>();
    private long boomerangUpdateCount;
    @Builder.Default
    private long timestamp = System.currentTimeMillis();
    private boolean reflow;
    private Map<String, Map<String, Object>> existingEnrichments;
}
