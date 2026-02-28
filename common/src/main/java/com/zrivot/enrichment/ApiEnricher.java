package com.zrivot.enrichment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zrivot.config.EnricherConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An enricher that calls an external REST API to fetch additional fields for a document.
 *
 * <p>Supports both synchronous ({@link #enrich}) and asynchronous ({@link #enrichAsync})
 * invocation.  The async path uses {@link HttpClient#sendAsync}, which lets Flink's
 * {@code AsyncDataStream} overlap many HTTP calls without blocking task threads.</p>
 *
 * <h3>Mapping</h3>
 * <p>Override {@link #mapToRequest} and {@link #mapFromResponse} in domain-specific
 * subclasses to control exactly what is sent to / received from the API.
 * The defaults send the whole payload and return the whole response.</p>
 *
 * <h3>Bulk support</h3>
 * <p>When {@code bulkEnabled} is {@code true}, override {@link #enrichBulk} to POST
 * a list of documents in a single HTTP call.  The default implementation wraps all
 * mapped request bodies into a JSON array, POSTs them, and expects a JSON array response.</p>
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code apiUrl} – the HTTP endpoint to POST the document payload to</li>
 *   <li>{@code timeoutMs} – HTTP request timeout in milliseconds (default: 5000)</li>
 *   <li>{@code bulkApiUrl} – optional separate endpoint for bulk requests (defaults to apiUrl)</li>
 * </ul>
 */
@Slf4j
public class ApiEnricher implements Enricher {

    private static final long serialVersionUID = 2L;

    private String apiUrl;
    private String bulkApiUrl;
    private int timeoutMs;
    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;

    @Override
    public void init(EnricherConfig config) {
        this.apiUrl = config.getProperty("apiUrl");
        this.bulkApiUrl = config.getProperty("bulkApiUrl", apiUrl);
        this.timeoutMs = Integer.parseInt(config.getProperty("timeoutMs", "5000"));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
        this.objectMapper = new ObjectMapper();
        log.info("Initialised ApiEnricher '{}' → {} (bulk: {})",
                config.getName(), apiUrl, config.isBulkEnabled());
    }
    
    // ── Single-document enrichment ───────────────────────────────────────

    @Override
    public Map<String, Object> enrich(String documentId, Map<String, Object> payload) throws Exception {
        ensureInitialised();

        Map<String, Object> requestBody = mapToRequest(documentId, payload);
        HttpRequest request = buildRequest(documentId, requestBody);
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        Map<String, Object> rawResponse = parseResponseBody(documentId, response);
        return mapFromResponse(documentId, payload, rawResponse);
    }

    @Override
    public CompletableFuture<Map<String, Object>> enrichAsync(String documentId,
                                                               Map<String, Object> payload) {
        ensureInitialised();

        Map<String, Object> requestBody = mapToRequest(documentId, payload);
        HttpRequest request;
        try {
            request = buildRequest(documentId, requestBody);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    Map<String, Object> rawResponse = parseResponseBody(documentId, response);
                    return mapFromResponse(documentId, payload, rawResponse);
                });
    }

    // ── Bulk enrichment ──────────────────────────────────────────────────

    @Override
    public boolean supportsBulk() {
        return true;  // ApiEnricher supports bulk by default via JSON array
    }

    @Override
    public List<Map<String, Object>> enrichBulk(List<String> documentIds,
                                                 List<Map<String, Object>> payloads) throws Exception {
        ensureInitialised();

        // Build array of mapped request bodies
        List<Map<String, Object>> requestBodies = new ArrayList<>();
        for (int i = 0; i < documentIds.size(); i++) {
            requestBodies.add(mapToRequest(documentIds.get(i), payloads.get(i)));
        }

        byte[] body = objectMapper.writeValueAsBytes(requestBodies);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(bulkApiUrl))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofMillis(timeoutMs))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("Bulk API call failed: status=" + response.statusCode()
                    + " body=" + response.body());
        }

        List<Map<String, Object>> rawResponses = objectMapper.readValue(
                response.body(), new TypeReference<>() {});

        // Map each response back
        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < documentIds.size(); i++) {
            Map<String, Object> raw = i < rawResponses.size() ? rawResponses.get(i) : Map.of();
            results.add(mapFromResponse(documentIds.get(i), payloads.get(i), raw));
        }
        return results;
    }

    @Override
    public CompletableFuture<List<Map<String, Object>>> enrichBulkAsync(
            List<String> documentIds, List<Map<String, Object>> payloads) {
        ensureInitialised();

        List<Map<String, Object>> requestBodies = new ArrayList<>();
        for (int i = 0; i < documentIds.size(); i++) {
            requestBodies.add(mapToRequest(documentIds.get(i), payloads.get(i)));
        }

        HttpRequest request;
        try {
            byte[] body = objectMapper.writeValueAsBytes(requestBodies);
            request = HttpRequest.newBuilder()
                    .uri(URI.create(bulkApiUrl))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofMillis(timeoutMs))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                        throw new RuntimeException("Bulk API failed: status=" + response.statusCode());
                    }
                    try {
                        List<Map<String, Object>> rawResponses = objectMapper.readValue(
                                response.body(), new TypeReference<>() {});

                        List<Map<String, Object>> results = new ArrayList<>();
                        for (int i = 0; i < documentIds.size(); i++) {
                            Map<String, Object> raw = i < rawResponses.size() ? rawResponses.get(i) : Map.of();
                            results.add(mapFromResponse(documentIds.get(i), payloads.get(i), raw));
                        }
                        return results;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse bulk response", e);
                    }
                });
    }

    @Override
    public void close() {
        log.info("Closed ApiEnricher for {}", apiUrl);
    }

    // ──────────────────────── internals ──────────────────────────────────

    private HttpRequest buildRequest(String documentId, Map<String, Object> requestBody) {
        ensureInitialised();
        try {
            byte[] body = objectMapper.writeValueAsBytes(requestBody);
            return HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .header("X-Document-Id", documentId)
                    .timeout(Duration.ofMillis(timeoutMs))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build enrichment request for doc=" + documentId, e);
        }
    }

    private Map<String, Object> parseResponseBody(String documentId, HttpResponse<String> response) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("API enrichment failed for doc=" + documentId +
                    " status=" + response.statusCode() + " body=" + response.body());
        }
        try {
            return objectMapper.readValue(response.body(), new TypeReference<>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse enrichment response for doc=" + documentId, e);
        }
    }

    /**
     * Re-creates transient fields after deserialisation.
     */
    private void ensureInitialised() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(timeoutMs))
                    .build();
        }
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
    }
}
