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
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawDocument implements Serializable {

    private static final long serialVersionUID = 1L;

    private String documentId;
    @Builder.Default
    private Map<String, Object> payload = new HashMap<>();
    private long boomerangUpdateCount;
    @Builder.Default
    private long timestamp = System.currentTimeMillis();
    private boolean reflow;
    private Map<String, Map<String, Object>> existingEnrichments;
}
