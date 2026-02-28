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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous Flink function that performs the actual enrichment API call without blocking
 * the task thread.
 *
 * <p>Placed <b>after</b> {@link BoomerangEnrichmentFunction} in the pipeline.  While the
 * boomerang guard needs keyed state (unavailable in async functions), this operator needs
 * only Flink's {@code AsyncDataStream} — which allows many HTTP calls to be in-flight
 * concurrently, massively improving throughput for I/O-bound enrichers.</p>
 *
 * <h3>Retry with re-fetch (reflow only)</h3>
 * <p>When enrichment fails for a <b>reflow</b> document, the function waits
 * {@code retryDelayMs} and then re-fetches the document from Elasticsearch to get the
 * latest version, before retrying enrichment.  This ensures the API always receives the
 * most up-to-date data.  After {@code maxRetries} attempts, a failure result is emitted.</p>
 *
 * <h3>Failure handling</h3>
 * <p>If the enricher call fails for a non-reflow document (or after max retries), a
 * {@link EnrichmentResult#failure} is emitted instead of propagating the error, so one
 * enricher failure does NOT block the joiner or other enrichers.</p>
 */
@Slf4j
public class AsyncEnrichmentFunction extends RichAsyncFunction<RawDocument, EnrichmentResult> {

    private static final long serialVersionUID = 3L;

    private final EnricherConfig enricherConfig;
    private final ElasticsearchConfig esConfig;
    private final String esIndex;

    private transient Enricher enricher;
    private transient ElasticsearchService esService;

    public AsyncEnrichmentFunction(EnricherConfig enricherConfig,
                                   ElasticsearchConfig esConfig,
                                   String esIndex) {
        this.enricherConfig = enricherConfig;
        this.esConfig = esConfig;
        this.esIndex = esIndex;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.enricher = EnricherFactory.create(enricherConfig);
        this.esService = new ElasticsearchService(esConfig);
    }

    @Override
    public void asyncInvoke(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture) {

        String documentId = doc.getDocumentId();
        long updateCount = doc.getBoomerangUpdateCount();

        CompletableFuture<Map<String, Object>> future =
                enricher.enrichAsync(documentId, doc.getPayload());

        future.whenComplete((enrichedFields, throwable) -> {
            if (throwable != null) {
                Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;

                // Retry with re-fetch for reflow documents
                if (doc.isReflow() && enricherConfig.getMaxRetries() > 0) {
                    log.warn("Enrichment failed for reflow doc={} enricher={}, scheduling retry (max {}): {}",
                            documentId, enricherConfig.getName(),
                            enricherConfig.getMaxRetries(), cause.getMessage());
                    retryWithRefetch(doc, resultFuture, enricherConfig.getMaxRetries());
                } else {
                    emitFailure(doc, resultFuture, cause.getMessage());
                }
            } else {
                emitSuccess(doc, resultFuture, enrichedFields);
            }
        });
    }

    /**
     * Retries enrichment after a delay, re-fetching the document from ES for fresh data.
     */
    private void retryWithRefetch(RawDocument originalDoc,
                                   ResultFuture<EnrichmentResult> resultFuture,
                                   int retriesLeft) {
        CompletableFuture.delayedExecutor(enricherConfig.getRetryDelayMs(), TimeUnit.MILLISECONDS)
                .execute(() -> {
                    try {
                        // Re-fetch from ES to get the most updated values
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

                        // Retry enrichment with fresh data
                        enricher.enrichAsync(freshDoc.getDocumentId(), freshDoc.getPayload())
                                .whenComplete((enrichedFields, throwable) -> {
                                    if (throwable != null) {
                                        Throwable cause = throwable.getCause() != null
                                                ? throwable.getCause() : throwable;

                                        if (retriesLeft > 1) {
                                            log.warn("Retry failed for doc={} enricher={}, " +
                                                            "{} retries left: {}",
                                                    freshDoc.getDocumentId(),
                                                    enricherConfig.getName(),
                                                    retriesLeft - 1, cause.getMessage());
                                            retryWithRefetch(freshDoc, resultFuture,
                                                    retriesLeft - 1);
                                        } else {
                                            log.error("All retries exhausted for doc={} enricher={}",
                                                    freshDoc.getDocumentId(),
                                                    enricherConfig.getName());
                                            emitFailure(freshDoc, resultFuture, cause.getMessage());
                                        }
                                    } else {
                                        emitSuccess(freshDoc, resultFuture, enrichedFields);
                                    }
                                });
                    } catch (Exception e) {
                        log.error("Re-fetch from ES failed for doc={}: {}",
                                originalDoc.getDocumentId(), e.getMessage(), e);
                        if (retriesLeft > 1) {
                            retryWithRefetch(originalDoc, resultFuture, retriesLeft - 1);
                        } else {
                            emitFailure(originalDoc, resultFuture,
                                    "Re-fetch failed: " + e.getMessage());
                        }
                    }
                });
    }

    private void emitSuccess(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture,
                              Map<String, Object> enrichedFields) {
        log.debug("Enrichment success: doc={} enricher={}",
                doc.getDocumentId(), enricherConfig.getName());
        resultFuture.complete(Collections.singleton(
                EnrichmentResult.success(
                        doc.getDocumentId(),
                        enricherConfig.getName(),
                        enrichedFields,
                        doc.getPayload(),
                        doc.getBoomerangUpdateCount(),
                        doc.isReflow(),
                        doc.getExistingEnrichments()
                )
        ));
    }

    private void emitFailure(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture,
                              String errorMessage) {
        log.error("Enrichment failed for doc={} enricher={}: {}",
                doc.getDocumentId(), enricherConfig.getName(), errorMessage);
        resultFuture.complete(Collections.singleton(
                EnrichmentResult.failure(
                        doc.getDocumentId(),
                        enricherConfig.getName(),
                        errorMessage,
                        doc.getPayload(),
                        doc.getBoomerangUpdateCount(),
                        doc.isReflow(),
                        doc.getExistingEnrichments()
                )
        ));
    }

    /**
     * Called when the async operation times out.  Emits a failure result rather than crashing.
     */
    @Override
    public void timeout(RawDocument doc, ResultFuture<EnrichmentResult> resultFuture) {
        String documentId = doc.getDocumentId();
        log.warn("Async enrichment timed out for doc={} enricher={}",
                documentId, enricherConfig.getName());

        resultFuture.complete(Collections.singleton(
                EnrichmentResult.failure(
                        documentId,
                        enricherConfig.getName(),
                        "Async enrichment timed out",
                        doc.getPayload(),
                        doc.getBoomerangUpdateCount(),
                        doc.isReflow(),
                        doc.getExistingEnrichments()
                )
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
