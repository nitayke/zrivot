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
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An enricher that calls an external REST API to fetch additional fields for a document.
 *
 * <p>Supports both synchronous ({@link #enrich}) and asynchronous ({@link #enrichAsync})
 * invocation.  The async path uses {@link HttpClient#sendAsync}, which lets Flink's
 * {@code AsyncDataStream} overlap many HTTP calls without blocking task threads.</p>
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code apiUrl} – the HTTP endpoint to POST the document payload to</li>
 *   <li>{@code timeoutMs} – HTTP request timeout in milliseconds (default: 5000)</li>
 * </ul>
 */
@Slf4j
public class ApiEnricher implements Enricher {

    private static final long serialVersionUID = 1L;

    private String apiUrl;
    private int timeoutMs;
    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;

    @Override
    public void init(EnricherConfig config) {
        this.apiUrl = config.getProperty("apiUrl");
        this.timeoutMs = Integer.parseInt(config.getProperty("timeoutMs", "5000"));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
        this.objectMapper = new ObjectMapper();
        log.info("Initialised ApiEnricher '{}' → {}", config.getName(), apiUrl);
    }

    @Override
    public Map<String, Object> enrich(String documentId, Object payload) throws Exception {
        ensureInitialised();

        HttpRequest request = buildRequest(documentId, payload);
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return parseResponse(documentId, response);
    }

    /**
     * Non-blocking enrichment using {@link HttpClient#sendAsync}.  Each call returns
     * immediately with a {@link CompletableFuture} — Flink's {@code AsyncDataStream}
     * manages multiple in-flight futures concurrently (up to the configured capacity).
     */
    @Override
    public CompletableFuture<Map<String, Object>> enrichAsync(String documentId,
                                                               Object payload) {
        ensureInitialised();

        HttpRequest request;
        try {
            request = buildRequest(documentId, payload);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> parseResponse(documentId, response));
    }

    @Override
    public void close() {
        // HttpClient doesn't need explicit close in JDK 17
        log.info("Closed ApiEnricher for {}", apiUrl);
    }

    // ──────────────────────── internals ──────────────────────────────────

    private HttpRequest buildRequest(String documentId, Object payload) {
        ensureInitialised();
        try {
            byte[] requestBody = objectMapper.writeValueAsBytes(payload);
            return HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .header("X-Document-Id", documentId)
                    .timeout(Duration.ofMillis(timeoutMs))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build enrichment request for doc=" + documentId, e);
        }
    }

    private Map<String, Object> parseResponse(String documentId, HttpResponse<String> response) {
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
