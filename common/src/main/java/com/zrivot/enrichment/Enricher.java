package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
     * Enriches the given document payload by fetching/computing additional fields (synchronous).
     *
     * @param documentId the unique document identifier
     * @param payload    the current document fields
     * @return a map of new enriched fields to merge into the document
     * @throws Exception if the enrichment fails (the pipeline isolates failures per-enricher)
     */
    Map<String, Object> enrich(String documentId, Map<String, Object> payload) throws Exception;

    /**
     * Asynchronous variant of {@link #enrich}.  Implementations that perform I/O (HTTP calls,
     * database lookups, etc.) should override this to use non-blocking clients, so that Flink's
     * {@code AsyncDataStream} can overlap multiple requests without blocking task threads.
     *
     * <p>The default implementation delegates to the synchronous {@link #enrich} method
     * on a common ForkJoinPool thread — suitable only when no true async client is available.</p>
     *
     * @param documentId the unique document identifier
     * @param payload    the current document fields
     * @return a future that completes with the enriched fields map
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

    /**
     * Releases resources held by this enricher.
     * Called when the Flink operator closes.
     */
    void close();
}
