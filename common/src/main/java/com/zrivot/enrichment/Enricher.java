package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

/**
 * Contract for all enrichment implementations.
 *
 * <p>Each enricher receives a document payload and returns a map of enriched fields.
 * Implementations must be serialisable (they travel across the Flink cluster) and should
 * acquire external resources (HTTP clients, etc.) inside {@link #init(EnricherConfig)}.</p>
 *
 * <h3>Mapping hooks</h3>
 * <p>Domain-specific enrichers override the mapping methods to transform raw document
 * payloads into API-specific request bodies, and to interpret API responses back into
 * enrichment fields.  Defaults pass the payload/response through as-is.</p>
 *
 * <h3>Reflow query mapping</h3>
 * <p>Each enricher that participates in reflow should override
 * {@link #buildReflowQuery(Map)} to convert the domain-level reflow criteria
 * (from the Kafka reflow topic) into an Elasticsearch query.</p>
 *
 * <h3>Bulk support</h3>
 * <p>Enrichers that call APIs accepting multiple documents per request should override
 * {@link #supportsBulk()} and {@link #enrichBulkAsync(List, List)}.</p>
 */
public interface Enricher extends Serializable {

    /**
     * Initialises the enricher with its configuration.
     * Called once when the Flink operator opens.
     */
    void init(EnricherConfig config);

    /**
     * Injects a shared {@link ExecutorService} for async I/O.
     *
     * <p>Called once after {@link #init} by the Flink operator.  Implementations
     * that create their own async primitives (e.g.&nbsp;{@code HttpClient}) should
     * rebuild them using this executor so that all enrichers on the same task-manager
     * share a single thread-pool.</p>
     *
     * @param executor the JVM-wide shared executor
     */
    default void configureExecutor(ExecutorService executor) {
        // no-op by default
    }

    // ── Mapping hooks ────────────────────────────────────────────────────

    /**
     * Transforms the raw document payload into the body to send to the enrichment API.
     * Override to select/reshape fields for the external service.
     *
     * @param documentId the unique document identifier
     * @param payload    the raw document payload map
     * @return the request body (default: payload as-is)
     */
    default Map<String, Object> mapToRequest(String documentId, Map<String, Object> payload) {
        return payload;
    }

    /**
     * Interprets the enrichment API response and returns the fields to merge back.
     * Override to rename/filter/transform response fields.
     *
     * @param documentId  the unique document identifier
     * @param payload     the original raw document payload
     * @param apiResponse the full API response parsed as a map
     * @return enriched fields to merge into the document (default: apiResponse as-is)
     */
    default Map<String, Object> mapFromResponse(String documentId,
                                                 Map<String, Object> payload,
                                                 Map<String, Object> apiResponse) {
        return apiResponse;
    }

    /**
     * Converts domain-level reflow criteria (from the Kafka reflow message) into an
     * Elasticsearch query.  Each enricher typically has its own mapping logic.
     *
     * <p>Override this per enricher to translate domain events (e.g. "geo data changed for
     * region X") into the appropriate ES query that selects affected documents.</p>
     *
     * @param reflowCriteria the raw criteria from the reflow Kafka message
     * @return an Elasticsearch query body as a map (default: reflowCriteria as-is)
     */
    default Map<String, Object> buildReflowQuery(Map<String, Object> reflowCriteria) {
        return reflowCriteria;
    }

    // ── Single-document enrichment ───────────────────────────────────────

    /**
     * Enriches the given document synchronously.
     *
     * <p>The default implementation of the async variants delegates to this method.</p>
     *
     * @param documentId the unique document identifier
     * @param payload    the document payload map
     * @return a map of new enriched fields to merge into the document
     */
    Map<String, Object> enrich(String documentId, Map<String, Object> payload) throws Exception;

    /**
     * Async single-document enrichment.  Implementations that perform I/O should
     * override this to use non-blocking clients.
     *
     * <p>Default delegates to the sync {@link #enrich} on the common ForkJoinPool.</p>
     */
    default CompletableFuture<Map<String, Object>> enrichAsync(String documentId,
                                                                Map<String, Object> payload) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return enrich(documentId, payload);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    // ── Bulk enrichment ──────────────────────────────────────────────────

    /**
     * Whether this enricher supports bulk (batch) API requests.
     */
    default boolean supportsBulk() {
        return false;
    }

    /**
     * Enriches a batch of documents in a single API call.
     *
     * <p>Returns a list of enriched-fields maps in the <b>same order</b> as the input
     * lists.  Returning {@code null} at index {@code i} means the enrichment failed for
     * that document.</p>
     *
     * @param documentIds document identifiers (parallel to {@code payloads})
     * @param payloads    document payload maps (parallel to {@code documentIds})
     * @return enriched-field maps, one per document, same order
     */
    default List<Map<String, Object>> enrichBulk(List<String> documentIds,
                                                  List<Map<String, Object>> payloads) throws Exception {
        throw new UnsupportedOperationException("Bulk enrichment not supported by this enricher");
    }

    /**
     * Async bulk enrichment.  Default delegates to sync {@link #enrichBulk}.
     */
    default CompletableFuture<List<Map<String, Object>>> enrichBulkAsync(
            List<String> documentIds, List<Map<String, Object>> payloads) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return enrichBulk(documentIds, payloads);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Releases resources held by this enricher.
     */
    void close();
}
