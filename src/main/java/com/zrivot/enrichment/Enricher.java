package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;

import java.io.Serializable;
import java.util.Map;

/**
 * Contract for all enrichment implementations.
 *
 * <p>Each enricher receives a document payload and returns a map of enriched fields.
 * Implementations must be serialisable (they travel across the Flink cluster) and should
 * acquire external resources (HTTP clients, etc.) inside {@link #init(EnricherConfig)}.</p>
 */
public interface Enricher extends Serializable {

    /**
     * Initialises the enricher with its configuration.
     * Called once when the Flink operator opens.
     */
    void init(EnricherConfig config);

    /**
     * Enriches the given document payload by fetching/computing additional fields.
     *
     * @param documentId the unique document identifier
     * @param payload    the current document fields
     * @return a map of new enriched fields to merge into the document
     * @throws Exception if the enrichment fails (the pipeline isolates failures per-enricher)
     */
    Map<String, Object> enrich(String documentId, Map<String, Object> payload) throws Exception;

    /**
     * Releases resources held by this enricher.
     * Called when the Flink operator closes.
     */
    void close();
}
