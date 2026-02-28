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

import com.zrivot.config.EnricherConfig;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Contract for all enrichment implementations.
 *
 * <p>Enrichers are composed from pluggable mapping strategies for:
 * <ul>
 *   <li>Elasticsearch query mapping</li>
 *   <li>API request mapping</li>
 *   <li>API response mapping</li>
 *   <li>Field name abstraction (DB field mapping)</li>
 * </ul>
 * Each mapping strategy can be implemented in a separate class for complex logic.</p>
 */
public interface Enricher extends Serializable {

    /** Initialises the enricher with its configuration. */
    void init(EnricherConfig config);

    /** Returns the Elasticsearch query mapper for reflow logic. */
    QueryMapper getQueryMapper();

    /** Returns the API request mapper. */
    ApiRequestMapper getApiRequestMapper();

    /** Returns the API response mapper. */
    ApiResponseMapper getApiResponseMapper();

    /** Returns the field mapping abstraction for DB field names. */
    FieldMapper getFieldMapper();

    // ── Single-document enrichment ───────────────────────────────────────

    /**
     * Enriches the given document synchronously.
     * Implementations should use the mapping strategies above.
     */
    Map<String, Object> enrich(String documentId, Map<String, Object> payload) throws Exception;

    /**
     * Async single-document enrichment.  Implementations that perform I/O should
     * override this to use non-blocking clients.
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

    default boolean supportsBulk() {
        return false;
    }

    default List<Map<String, Object>> enrichBulk(List<String> documentIds,
                                                  List<Map<String, Object>> payloads) throws Exception {
        throw new UnsupportedOperationException("Bulk enrichment not supported by this enricher");
    }

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

    void close();

    // ── Mapping strategy interfaces ─────────────────────────────────────

    interface QueryMapper extends Serializable {
        Map<String, Object> mapReflowQuery(Map<String, Object> reflowCriteria);
    }

    interface ApiRequestMapper extends Serializable {
        Map<String, Object> mapToRequest(String documentId, Map<String, Object> payload);
    }

    interface ApiResponseMapper extends Serializable {
        Map<String, Object> mapFromResponse(String documentId, Map<String, Object> payload, Map<String, Object> apiResponse);
    }

    interface FieldMapper extends Serializable {
        /**
         * Maps a logical field name to the DB-specific field name for the current domain.
         * E.g. "storeId" → "store_id" (purchases), "storeId" → "client_store_id" (clients)
         */
        String mapField(String logicalFieldName);
    }
}