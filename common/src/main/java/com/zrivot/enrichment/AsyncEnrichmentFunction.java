package com.zrivot.enrichment;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.EnricherConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.connector.source.util.ratelimit.GuavaRateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.NoOpRateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous Flink function that performs single-document enrichment API calls
 * without blocking the task thread.
 *
 * <h3>Rate limiting (point 1)</h3>
 * <p>Each enricher has a configurable {@code rateLimitPerSecond}.  The operator uses
 * Flink's built-in {@link RateLimiter} — a {@link GuavaRateLimiter} when the rate is
 * positive, or a {@link NoOpRateLimiter} when rate limiting is disabled ({@code 0}).
 * The rate-limit acquire runs on the Flink task thread, and HTTP client connection pooling
 * is handled by the built-in HttpClient. Each enricher manages its own rate limit and connection pool.</p>
 *
 * <h3>Retry with re-fetch (reflow only)</h3>
 * <p>When enrichment fails for a <b>reflow</b> document, the function schedules a
 * delayed retry: wait {@code retryDelayMs} → re-fetch from ES → retry enrichment.
 * After {@code maxRetries} a failure result is emitted.</p>
 */
@Slf4j
public class AsyncEnrichmentFunction extends RichAsyncFunction<RawDocument, EnrichmentResult> {

    private static final long serialVersionUID = 5L;

    private final EnricherConfig enricherConfig;
    private final ElasticsearchConfig esConfig;
    private final String esIndex;
    private final int maxConcurrentApiRequests;

    private transient Enricher enricher;
    private transient ElasticsearchService esService;
    private transient RateLimiter rateLimiter;

    public AsyncEnrichmentFunction(EnricherConfig enricherConfig,
                                   ElasticsearchConfig esConfig,
                                   String esIndex,
                                   int maxConcurrentApiRequests) {
        this.enricherConfig = enricherConfig;
        this.esConfig = esConfig;
        this.esIndex = esIndex;
        this.maxConcurrentApiRequests = maxConcurrentApiRequests;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.enricher = EnricherFactory.create(enricherConfig);
        this.esService = new ElasticsearchService(esConfig);
        int ratePerSec = enricherConfig.getRateLimitPerSecond();
        this.rateLimiter = ratePerSec > 0
            ? new GuavaRateLimiter(ratePerSec)
            : new NoOpRateLimiter();
    }

    // ── asyncInvoke ──────────────────────────────────────────────────────

    @Override
    public void asyncInvoke(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture) {
        rateLimiter.acquire().toCompletableFuture().thenRunAsync(() ->
                enricher.enrichAsync(doc.getDocumentId(), doc.getPayload())
                        .whenComplete((enrichedFields, throwable) -> {
                            handleResult(doc, resultFuture, enrichedFields, throwable);
                        })
        );
    }

    private void handleResult(RawDocument doc,
                              ResultFuture<EnrichmentResult> resultFuture,
                              Map<String, Object> enrichedFields,
                              Throwable throwable) {
        if (throwable != null) {
            Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;

            if (doc.isReflow() && enricherConfig.getMaxRetries() > 0) {
                log.warn("Enrichment failed for reflow doc={} enricher={}, scheduling retry (max {}): {}",
                        doc.getDocumentId(), enricherConfig.getName(),
                        enricherConfig.getMaxRetries(), cause.getMessage());
                retryWithRefetch(doc, resultFuture, enricherConfig.getMaxRetries());
            } else {
                emitFailure(doc, resultFuture, cause.getMessage());
            }
        } else {
            emitSuccess(doc, resultFuture, enrichedFields);
        }
    }

    // ── Retry with re-fetch ──────────────────────────────────────────────

    private void retryWithRefetch(RawDocument originalDoc,
                                  ResultFuture<EnrichmentResult> resultFuture,
                                  int retriesLeft) {
        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(enricherConfig.getRetryDelayMs());

                var freshDocs = esService.fetchByIds(esIndex,
                        Collections.singletonList(originalDoc.getDocumentId()));

                if (freshDocs.isEmpty()) {
                    log.warn("Document {} no longer exists in ES, emitting failure",
                            originalDoc.getDocumentId());
                    emitFailure(originalDoc, resultFuture,
                            "Document not found on re-fetch from ES");
                    return;
                }

                RawDocument freshDoc = freshDocs.get(0);
                freshDoc.setReflow(true);
                freshDoc.setExistingEnrichments(originalDoc.getExistingEnrichments());

                // Rate-limit the retry as well
                rateLimiter.acquire().toCompletableFuture().thenRunAsync(() ->
                        enricher.enrichAsync(freshDoc.getDocumentId(), freshDoc.getPayload())
                                .whenComplete((enrichedFields, throwable) -> {
                                    if (throwable != null) {
                                        Throwable cause = throwable.getCause() != null
                                                ? throwable.getCause() : throwable;
                                        if (retriesLeft > 1) {
                                            log.warn("Retry failed for doc={} enricher={}, {} retries left: {}",
                                                    freshDoc.getDocumentId(), enricherConfig.getName(),
                                                    retriesLeft - 1, cause.getMessage());
                                            retryWithRefetch(freshDoc, resultFuture, retriesLeft - 1);
                                        } else {
                                            log.error("All retries exhausted for doc={} enricher={}",
                                                    freshDoc.getDocumentId(), enricherConfig.getName());
                                            emitFailure(freshDoc, resultFuture, cause.getMessage());
                                        }
                                    } else {
                                        emitSuccess(freshDoc, resultFuture, enrichedFields);
                                    }
                                })
                );
            } catch (Exception e) {
                log.error("Re-fetch from ES failed for doc={}: {}",
                        originalDoc.getDocumentId(), e.getMessage(), e);
                if (retriesLeft > 1) {
                    retryWithRefetch(originalDoc, resultFuture, retriesLeft - 1);
                } else {
                    emitFailure(originalDoc, resultFuture, "Re-fetch failed: " + e.getMessage());
                }
            }
        }).start();
    }

    // ── Emit helpers ─────────────────────────────────────────────────────

    private void emitSuccess(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture,
                             Map<String, Object> enrichedFields) {
        log.debug("Enrichment success: doc={} enricher={}",
                doc.getDocumentId(), enricherConfig.getName());
        safeComplete(resultFuture, EnrichmentResult.success(
                doc.getDocumentId(),
                enricherConfig.getName(),
                enrichedFields,
                doc.getPayload(),
                doc.getBoomerangUpdateCount(),
                doc.isReflow(),
                doc.getExistingEnrichments()
        ));
    }

    private void emitFailure(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture,
                             String errorMessage) {
        log.error("Enrichment failed for doc={} enricher={}: {}",
                doc.getDocumentId(), enricherConfig.getName(), errorMessage);
        safeComplete(resultFuture, EnrichmentResult.failure(
                doc.getDocumentId(),
                enricherConfig.getName(),
                errorMessage,
                doc.getPayload(),
                doc.getBoomerangUpdateCount(),
                doc.isReflow(),
                doc.getExistingEnrichments()
        ));
    }

    /** Completes a result future, silently ignoring double-completion (e.g. after timeout). */
    private void safeComplete(ResultFuture<EnrichmentResult> rf, EnrichmentResult result) {
        try {
            rf.complete(Collections.singleton(result));
        } catch (Exception e) {
            log.debug("ResultFuture already completed, ignoring: {}", e.getMessage());
        }
    }

    // ── Timeout ──────────────────────────────────────────────────────────

    @Override
    public void timeout(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture) {
        log.warn("Async enrichment timed out for doc={} enricher={}",
                doc.getDocumentId(), enricherConfig.getName());
        safeComplete(resultFuture, EnrichmentResult.failure(
                doc.getDocumentId(),
                enricherConfig.getName(),
                "Async enrichment timed out",
                doc.getPayload(),
                doc.getBoomerangUpdateCount(),
                doc.isReflow(),
                doc.getExistingEnrichments()
        ));
    }

    @Override
    public void close() throws Exception {
        if (enricher != null) {
            enricher.close();
        }
        if (esService != null) {
            esService.close();
        }
        super.close();
    }
}
