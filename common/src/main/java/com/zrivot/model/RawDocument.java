package com.zrivot.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a raw document entering the enrichment pipeline.
 * Can originate from the Kafka raw topic (realtime) or from Elasticsearch (reflow).
 *
 * <p>The type parameter {@code T} is the domain-specific document type
 * (e.g. {@code PurchaseDocument}, {@code ClientDocument}).  The payload is
 * deserialized from JSON at the pipeline boundaries (Kafka source / ES fetch)
 * and carried as a typed object for type-safe access throughout the pipeline.</p>
 *
 * @param <T> the domain document type
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawDocument<T> implements Serializable {

    private static final long serialVersionUID = 2L;

    private String documentId;
    private T payload;
    private long boomerangUpdateCount;
    @Builder.Default
    private long timestamp = System.currentTimeMillis();
    private boolean reflow;
    private Map<String, Map<String, Object>> existingEnrichments;
}
