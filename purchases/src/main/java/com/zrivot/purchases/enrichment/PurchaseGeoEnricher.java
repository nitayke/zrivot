package com.zrivot.purchases.enrichment;

import com.zrivot.enrichment.ApiEnricher;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Purchase-domain geo-enricher that demonstrates <b>all</b> mapping hooks provided by
 * the {@link com.zrivot.enrichment.Enricher} interface.
 *
 * <h3>How to implement your own enricher</h3>
 * <ol>
 *   <li><b>Extend {@link ApiEnricher}</b> — it provides HTTP transport, bulk support,
 *       and the shared-executor integration out of the box.</li>
 *   <li><b>Override {@link #mapToRequest}</b> — extract only the fields that the
 *       external API needs from the raw document payload.</li>
 *   <li><b>Override {@link #mapFromResponse}</b> — transform the API response into
 *       the exact fields you want to merge back into the document.</li>
 *   <li><b>Override {@link #buildReflowQuery}</b> — translate the domain-level reflow
 *       criteria (e.g. "region X geo data changed") into an Elasticsearch query that
 *       selects affected documents.</li>
 * </ol>
 *
 * <h3>What this enricher does</h3>
 * <p>For each purchase document it sends the store ID and country to a Geo Service
 * and receives back coordinates, region, timezone, etc.  On reflow, it converts a
 * "region changed" event into an ES query that matches all purchases in that region.</p>
 *
 * <h3>Request payload sent to Geo API</h3>
 * <pre>{@code
 * {
 *   "storeId":  "STORE-42",
 *   "country":  "US"
 * }
 * }</pre>
 *
 * <h3>Response from Geo API (example)</h3>
 * <pre>{@code
 * {
 *   "latitude":  40.7128,
 *   "longitude": -74.0060,
 *   "region":    "US-NY",
 *   "timezone":  "America/New_York",
 *   "city":      "New York"
 * }
 * }</pre>
 *
 * <h3>Enrichment fields merged into the document</h3>
 * <pre>{@code
 * {
 *   "geo_latitude":  40.7128,
 *   "geo_longitude": -74.0060,
 *   "geo_region":    "US-NY",
 *   "geo_timezone":  "America/New_York"
 * }
 * }</pre>
 */
@Slf4j
public class PurchaseGeoEnricher extends ApiEnricher {

    private static final long serialVersionUID = 1L;

    // ── mapToRequest ─────────────────────────────────────────────────────
    //
    //   Extracts only the fields the Geo API needs from the purchase payload.
    //   This keeps the API request small and focused.

    @Override
    public Map<String, Object> mapToRequest(String documentId, Map<String, Object> payload) {
        Map<String, Object> request = new HashMap<>();

        // The Geo API needs the store ID and country to resolve coordinates
        request.put("storeId", payload.getOrDefault("storeId", ""));
        request.put("country", payload.getOrDefault("currency", ""));  // currency as country proxy

        // Optionally include the purchase date so the API can use historical geo data
        if (payload.containsKey("purchaseDate")) {
            request.put("referenceDate", payload.get("purchaseDate"));
        }

        log.debug("mapToRequest: doc={} → {}", documentId, request);
        return request;
    }

    // ── mapFromResponse ──────────────────────────────────────────────────
    //
    //   Transforms the Geo API response into the exact enrichment fields to
    //   merge back into the document.  We prefix all fields with "geo_" to
    //   avoid naming collisions with the raw document.

    @Override
    public Map<String, Object> mapFromResponse(String documentId,
                                               Map<String, Object> payload,
                                               Map<String, Object> apiResponse) {
        Map<String, Object> enriched = new HashMap<>();

        // Map response fields to our enrichment namespace
        enriched.put("geo_latitude", apiResponse.get("latitude"));
        enriched.put("geo_longitude", apiResponse.get("longitude"));
        enriched.put("geo_region", apiResponse.get("region"));
        enriched.put("geo_timezone", apiResponse.get("timezone"));

        // Derived field: combine latitude + longitude into a geo_point string
        // (useful for Elasticsearch geo queries)
        Object lat = apiResponse.get("latitude");
        Object lon = apiResponse.get("longitude");
        if (lat != null && lon != null) {
            enriched.put("geo_location", lat + "," + lon);
        }

        log.debug("mapFromResponse: doc={} → {} enrichment fields", documentId, enriched.size());
        return enriched;
    }

    // ── buildReflowQuery ─────────────────────────────────────────────────
    //
    //   Converts domain-level reflow criteria into an Elasticsearch query.
    //
    //   The Kafka reflow message might look like:
    //   {
    //     "enricherName": "purchase-geo-enricher",
    //     "queryCriteria": {
    //       "region": "US-NY",
    //       "changeType": "geo_data_updated"
    //     }
    //   }
    //
    //   This method converts that into an ES query:
    //   {
    //     "bool": {
    //       "must": [
    //         { "term": { "enrichments.purchase-geo-enricher.geo_region": "US-NY" } }
    //       ]
    //     }
    //   }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> buildReflowQuery(Map<String, Object> reflowCriteria) {
        Map<String, Object> query = new HashMap<>();

        String region = (String) reflowCriteria.get("region");
        String storeId = (String) reflowCriteria.get("storeId");

        // Build a bool query with one or more conditions
        List<Map<String, Object>> mustClauses = new java.util.ArrayList<>();

        if (region != null && !region.isBlank()) {
            // Match all purchases that were previously enriched with this region
            mustClauses.add(Map.of(
                    "term", Map.of(
                            "enrichments.purchase-geo-enricher.geo_region", region
                    )
            ));
        }

        if (storeId != null && !storeId.isBlank()) {
            // Match all purchases from a specific store
            mustClauses.add(Map.of(
                    "term", Map.of("storeId", storeId)
            ));
        }

        if (mustClauses.isEmpty()) {
            // If no specific criteria, match all documents (full reflow)
            query.put("match_all", Map.of());
        } else {
            query.put("bool", Map.of("must", mustClauses));
        }

        log.info("buildReflowQuery: criteria={} → ES query={}", reflowCriteria, query);
        return query;
    }
}
