package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous Flink function that performs the actual enrichment API call without blocking
 * the task thread.
 *
 * <p>Placed <b>after</b> {@link BoomerangEnrichmentFunction} in the pipeline.  While the
 * boomerang guard needs keyed state (unavailable in async functions), this operator needs
 * only Flink's {@code AsyncDataStream} — which allows many HTTP calls to be in-flight
 * concurrently, massively improving throughput for I/O-bound enrichers.</p>
 *
 * <h3>Failure handling</h3>
 * <p>If the enricher call fails (exception, timeout, non-2xx response), a
 * {@link EnrichmentResult#failure} is emitted instead of propagating the error, so one
 * enricher failure does NOT block the joiner or other enrichers.</p>
 */
@Slf4j
public class AsyncEnrichmentFunction extends RichAsyncFunction<RawDocument<?>, EnrichmentResult> {

    private static final long serialVersionUID = 2L;

    private final EnricherConfig enricherConfig;

    private transient Enricher enricher;

    public AsyncEnrichmentFunction(EnricherConfig enricherConfig) {
        this.enricherConfig = enricherConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.enricher = EnricherFactory.create(enricherConfig);
    }

    @Override
    public void asyncInvoke(RawDocument<?> doc, ResultFuture<EnrichmentResult> resultFuture) {

        String documentId = doc.getDocumentId();
        long updateCount = doc.getBoomerangUpdateCount();

        CompletableFuture<Map<String, Object>> future = enricher.enrichAsync(documentId, doc.getPayload());

        future.whenComplete((enrichedFields, throwable) -> {
            if (throwable != null) {
                // Unwrap CompletionException if present
                Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                log.error("Async enrichment failed for doc={} enricher={}: {}",
                        documentId, enricherConfig.getName(), cause.getMessage(), cause);

                resultFuture.complete(Collections.singleton(
                        EnrichmentResult.failure(
                                documentId,
                                enricherConfig.getName(),
                                cause.getMessage(),
                                doc.getPayload(),
                                updateCount,
                                doc.isReflow(),
                                doc.getExistingEnrichments()
                        )
                ));
            } else {
                log.debug("Async enrichment success: doc={} enricher={}",
                        documentId, enricherConfig.getName());

                resultFuture.complete(Collections.singleton(
                        EnrichmentResult.success(
                                documentId,
                                enricherConfig.getName(),
                                enrichedFields,
                                doc.getPayload(),
                                updateCount,
                                doc.isReflow(),
                                doc.getExistingEnrichments()
                        )
                ));
            }
        });
    }

    /**
     * Called when the async operation times out (exceeds the timeout configured in
     * {@code AsyncDataStream}).  Emits a failure result rather than crashing the pipeline.
     */
    @Override
    public void timeout(RawDocument<?> doc, ResultFuture<EnrichmentResult> resultFuture) {

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
        super.close();
    }
}
