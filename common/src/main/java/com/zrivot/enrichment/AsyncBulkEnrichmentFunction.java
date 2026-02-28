package com.zrivot.enrichment;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.EnricherConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Async bulk enrichment function used with {@code AsyncDataStream.orderedWait()}.
 *
 * <p>Documents arriving via {@link #asyncInvoke} are accumulated in an internal buffer.
 * The buffer is flushed — sending a single bulk API request — when <b>either</b>:
 * <ul>
 *   <li>The buffer reaches {@code bulkSize}, or</li>
 *   <li>A 2-second flush timer fires (for partial batches).</li>
 * </ul>
 *
 * <h3>Why this is an {@code RichAsyncFunction} (points 2 &amp; 3)</h3>
 * <p>Using {@code AsyncDataStream.orderedWait()} (instead of the previous
 * {@code KeyedProcessFunction + keyBy("bulk")}) gives us:</p>
 * <ul>
 *   <li><b>Async I/O</b> — the bulk API call runs off the Flink task thread.</li>
 *   <li><b>Order preservation</b> — orderedWait emits results in input order.</li>
 *   <li><b>Parallel distribution</b> — no {@code keyBy("bulk")} means the stream is
 *       <em>not</em> forced onto a single key/subtask; Flink distributes work across
 *       all task slots.</li>
 * </ul>
 *
 * <h3>Rate limiting (point 1)</h3>
 * <p>Each bulk request counts as <b>one</b> API request toward the enricher's
 * per-operator rate limit.</p>
 *
 * <h3>Concurrency control (point 5)</h3>
 * <p>The JVM-wide {@link SharedEnrichmentExecutor} semaphore limits concurrent API
 * requests across all enrichers on the same task manager.</p>
 *
 * <h3>Retry with re-fetch (reflow only)</h3>
 * <p>On failure for reflow documents: wait → re-fetch from ES → retry bulk call.</p>
 */
@Slf4j
public class AsyncBulkEnrichmentFunction
        extends RichAsyncFunction<RawDocument, EnrichmentResult> {

    private static final long serialVersionUID = 1L;

    /** How long to wait before flushing a partial batch (ms). */
    private static final long FLUSH_DELAY_MS = 2_000;

    private final EnricherConfig enricherConfig;
    private final ElasticsearchConfig esConfig;
    private final String esIndex;
    private final int maxConcurrentApiRequests;

    private transient Enricher enricher;
    private transient ElasticsearchService esService;
    private transient EnrichmentRateLimiter rateLimiter;

    // ── Internal buffer (synchronised on `lock`) ──────────────────────
    private final Object lock = new Object();
    private transient List<RawDocument> buffer;
    private transient List<ResultFuture<EnrichmentResult>> pendingFutures;
    private transient ScheduledExecutorService flushScheduler;
    private transient ScheduledFuture<?> pendingFlush;

    public AsyncBulkEnrichmentFunction(EnricherConfig enricherConfig,
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
        SharedEnrichmentExecutor.initialize(maxConcurrentApiRequests);

        this.enricher = EnricherFactory.create(enricherConfig);
        this.enricher.configureExecutor(SharedEnrichmentExecutor.executor());
        this.esService = new ElasticsearchService(esConfig);
        this.rateLimiter = new EnrichmentRateLimiter(enricherConfig.getRateLimitPerSecond());

        this.buffer = new ArrayList<>();
        this.pendingFutures = new ArrayList<>();
        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bulk-flush-" + enricherConfig.getName());
            t.setDaemon(true);
            return t;
        });
    }

    // ── asyncInvoke ──────────────────────────────────────────────────────

    @Override
    public void asyncInvoke(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture) {
        synchronized (lock) {
            buffer.add(doc);
            pendingFutures.add(resultFuture);

            if (buffer.size() >= enricherConfig.getBulkSize()) {
                cancelPendingFlush();
                flushBatch();
            } else if (pendingFlush == null) {
                pendingFlush = flushScheduler.schedule(() -> {
                    synchronized (lock) {
                        flushBatch();
                    }
                }, FLUSH_DELAY_MS, TimeUnit.MILLISECONDS);
            }
        }
    }

    /** Drains the current buffer and fires an async bulk enrichment call. */
    private void flushBatch() {
        // Caller must hold `lock`
        if (buffer.isEmpty()) return;

        List<RawDocument> batch = new ArrayList<>(buffer);
        List<ResultFuture<EnrichmentResult>> futures = new ArrayList<>(pendingFutures);
        buffer.clear();
        pendingFutures.clear();
        cancelPendingFlush();

        log.debug("Flushing bulk batch of {} documents for enricher '{}'",
                batch.size(), enricherConfig.getName());

        List<String> docIds = batch.stream()
                .map(RawDocument::getDocumentId).collect(Collectors.toList());
        List<Map<String, Object>> payloads = batch.stream()
                .map(RawDocument::getPayload).collect(Collectors.toList());

        long waitMicros = rateLimiter.reservePermitMicros();

        SharedEnrichmentExecutor.scheduleWithConcurrency(waitMicros, () ->
                enricher.enrichBulkAsync(docIds, payloads)
                        .whenComplete((results, throwable) -> {
                            SharedEnrichmentExecutor.releaseConcurrencyPermit();

                            if (throwable != null) {
                                Throwable cause = throwable.getCause() != null
                                        ? throwable.getCause() : throwable;
                                boolean anyReflow = batch.stream().anyMatch(RawDocument::isReflow);

                                if (anyReflow && enricherConfig.getMaxRetries() > 0) {
                                    log.warn("Bulk enrichment failed for enricher '{}', " +
                                                    "scheduling retry: {}",
                                            enricherConfig.getName(), cause.getMessage());
                                    retryWithRefetch(batch, futures,
                                            enricherConfig.getMaxRetries());
                                } else {
                                    emitFailures(batch, futures, cause.getMessage());
                                }
                            } else {
                                emitBulkResults(batch, futures, results);
                            }
                        })
        );
    }

    // ── Retry with re-fetch ──────────────────────────────────────────────

    private void retryWithRefetch(List<RawDocument> batch,
                                  List<ResultFuture<EnrichmentResult>> futures,
                                  int retriesLeft) {
        SharedEnrichmentExecutor.executor().execute(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(enricherConfig.getRetryDelayMs());

                List<String> docIds = batch.stream()
                        .map(RawDocument::getDocumentId).collect(Collectors.toList());
                List<RawDocument> freshDocs = esService.fetchByIds(esIndex, docIds);

                if (freshDocs.isEmpty()) {
                    emitFailures(batch, futures,
                            "Documents not found on re-fetch from ES");
                    return;
                }

                // Preserve metadata from originals
                Map<String, RawDocument> origMap = new HashMap<>();
                for (RawDocument orig : batch) {
                    origMap.put(orig.getDocumentId(), orig);
                }
                for (RawDocument freshDoc : freshDocs) {
                    freshDoc.setReflow(true);
                    RawDocument orig = origMap.get(freshDoc.getDocumentId());
                    if (orig != null) {
                        freshDoc.setExistingEnrichments(orig.getExistingEnrichments());
                    }
                }

                List<String> freshIds = freshDocs.stream()
                        .map(RawDocument::getDocumentId).collect(Collectors.toList());
                List<Map<String, Object>> freshPayloads = freshDocs.stream()
                        .map(RawDocument::getPayload).collect(Collectors.toList());

                long waitMicros = rateLimiter.reservePermitMicros();

                SharedEnrichmentExecutor.scheduleWithConcurrency(waitMicros, () ->
                        enricher.enrichBulkAsync(freshIds, freshPayloads)
                                .whenComplete((results, throwable) -> {
                                    SharedEnrichmentExecutor.releaseConcurrencyPermit();

                                    if (throwable != null) {
                                        if (retriesLeft > 1) {
                                            retryWithRefetch(batch, futures,
                                                    retriesLeft - 1);
                                        } else {
                                            Throwable c = throwable.getCause() != null
                                                    ? throwable.getCause() : throwable;
                                            emitFailures(batch, futures, c.getMessage());
                                        }
                                    } else {
                                        emitBulkResultsByDocId(batch, futures,
                                                freshDocs, results);
                                    }
                                })
                );
            } catch (Exception e) {
                log.error("Re-fetch failed for bulk enricher '{}': {}",
                        enricherConfig.getName(), e.getMessage(), e);
                if (retriesLeft > 1) {
                    retryWithRefetch(batch, futures, retriesLeft - 1);
                } else {
                    emitFailures(batch, futures, "Re-fetch failed: " + e.getMessage());
                }
            }
        });
    }

    // ── Emit helpers ─────────────────────────────────────────────────────

    /**
     * Pairs results by index — used when the batch and results list are parallel.
     */
    private void emitBulkResults(List<RawDocument> batch,
                                 List<ResultFuture<EnrichmentResult>> futures,
                                 List<Map<String, Object>> results) {
        for (int i = 0; i < batch.size(); i++) {
            RawDocument doc = batch.get(i);
            Map<String, Object> enriched = (i < results.size()) ? results.get(i) : null;

            if (enriched != null) {
                safeComplete(futures.get(i), EnrichmentResult.success(
                        doc.getDocumentId(), enricherConfig.getName(), enriched,
                        doc.getPayload(), doc.getBoomerangUpdateCount(),
                        doc.isReflow(), doc.getExistingEnrichments()));
            } else {
                safeComplete(futures.get(i), EnrichmentResult.failure(
                        doc.getDocumentId(), enricherConfig.getName(),
                        "Bulk enrichment returned null for this document",
                        doc.getPayload(), doc.getBoomerangUpdateCount(),
                        doc.isReflow(), doc.getExistingEnrichments()));
            }
        }
    }

    /**
     * Maps results back by document ID — used after re-fetch where the fresh batch
     * may have a different ordering or different size.
     */
    private void emitBulkResultsByDocId(
            List<RawDocument> originalBatch,
            List<ResultFuture<EnrichmentResult>> futures,
            List<RawDocument> freshDocs,
            List<Map<String, Object>> results) {

        // Build a lookup: docId → enriched fields
        Map<String, Map<String, Object>> resultsByDocId = new HashMap<>();
        for (int i = 0; i < freshDocs.size(); i++) {
            if (i < results.size() && results.get(i) != null) {
                resultsByDocId.put(freshDocs.get(i).getDocumentId(), results.get(i));
            }
        }

        for (int i = 0; i < originalBatch.size(); i++) {
            RawDocument doc = originalBatch.get(i);
            Map<String, Object> enriched = resultsByDocId.get(doc.getDocumentId());

            if (enriched != null) {
                safeComplete(futures.get(i), EnrichmentResult.success(
                        doc.getDocumentId(), enricherConfig.getName(), enriched,
                        doc.getPayload(), doc.getBoomerangUpdateCount(),
                        doc.isReflow(), doc.getExistingEnrichments()));
            } else {
                safeComplete(futures.get(i), EnrichmentResult.failure(
                        doc.getDocumentId(), enricherConfig.getName(),
                        "Document not found in re-fetch results",
                        doc.getPayload(), doc.getBoomerangUpdateCount(),
                        doc.isReflow(), doc.getExistingEnrichments()));
            }
        }
    }

    private void emitFailures(List<RawDocument> batch,
                              List<ResultFuture<EnrichmentResult>> futures,
                              String errorMessage) {
        for (int i = 0; i < batch.size(); i++) {
            RawDocument doc = batch.get(i);
            safeComplete(futures.get(i), EnrichmentResult.failure(
                    doc.getDocumentId(), enricherConfig.getName(), errorMessage,
                    doc.getPayload(), doc.getBoomerangUpdateCount(),
                    doc.isReflow(), doc.getExistingEnrichments()));
        }
    }

    /** Completes a ResultFuture, silently ignoring double-completion (e.g. after timeout). */
    private void safeComplete(ResultFuture<EnrichmentResult> rf, EnrichmentResult result) {
        try {
            rf.complete(Collections.singleton(result));
        } catch (Exception e) {
            log.debug("ResultFuture already completed, ignoring: {}", e.getMessage());
        }
    }

    // ── Timer helpers ────────────────────────────────────────────────────

    private void cancelPendingFlush() {
        if (pendingFlush != null) {
            pendingFlush.cancel(false);
            pendingFlush = null;
        }
    }

    // ── Timeout ──────────────────────────────────────────────────────────

    @Override
    public void timeout(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture) {
        log.warn("Async bulk enrichment timed out for doc={} enricher={}",
                doc.getDocumentId(), enricherConfig.getName());
        safeComplete(resultFuture, EnrichmentResult.failure(
                doc.getDocumentId(), enricherConfig.getName(),
                "Async bulk enrichment timed out",
                doc.getPayload(), doc.getBoomerangUpdateCount(),
                doc.isReflow(), doc.getExistingEnrichments()));
    }

    @Override
    public void close() throws Exception {
        if (flushScheduler != null) {
            flushScheduler.shutdownNow();
        }
        if (enricher != null) {
            enricher.close();
        }
        if (esService != null) {
            esService.close();
        }
        super.close();
    }
}
