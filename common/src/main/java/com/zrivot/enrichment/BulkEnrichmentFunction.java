package com.zrivot.enrichment;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.EnricherConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Buffered batch enrichment function for enrichers that support bulk API requests.
 *
 * <p>Incoming documents are accumulated in Flink keyed state.  The buffer is flushed (and
 * the enricher's {@link Enricher#enrichBulk} is called) when either:
 * <ul>
 *   <li>The buffer reaches {@code bulkSize} documents, or</li>
 *   <li>A processing-time timer fires (5 seconds after the first buffered element),
 *       ensuring partial batches are not stuck indefinitely.</li>
 * </ul>
 *
 * <h3>Retry with re-fetch (reflow only)</h3>
 * <p>When a bulk enrichment call fails for reflow documents, the function waits
 * {@code retryDelayMs}, re-fetches the documents from Elasticsearch to obtain
 * the most up-to-date values, and retries.  After {@code maxRetries} attempts a
 * failure result is emitted for every document in the batch.</p>
 */
@Slf4j
public class BulkEnrichmentFunction
        extends KeyedProcessFunction<String, RawDocument, EnrichmentResult> {

    private static final long serialVersionUID = 1L;

    /** How long to wait (ms) before flushing a partial batch. */
    private static final long FLUSH_TIMER_INTERVAL_MS = 5_000;

    private final EnricherConfig enricherConfig;
    private final ElasticsearchConfig esConfig;
    private final String esIndex;

    private transient Enricher enricher;
    private transient ElasticsearchService esService;

    private transient ListState<RawDocument> bufferState;
    private transient boolean timerRegistered;

    public BulkEnrichmentFunction(EnricherConfig enricherConfig,
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

        ListStateDescriptor<RawDocument> descriptor =
                new ListStateDescriptor<>("bulk-buffer", RawDocument.class);
        this.bufferState = getRuntimeContext().getListState(descriptor);
        this.timerRegistered = false;
    }

    @Override
    public void processElement(RawDocument doc, Context ctx, Collector<EnrichmentResult> out)
            throws Exception {

        bufferState.add(doc);

        // Count buffered documents
        int size = 0;
        for (@SuppressWarnings("unused") RawDocument ignored : bufferState.get()) {
            size++;
        }

        if (size >= enricherConfig.getBulkSize()) {
            flushBuffer(out);
            timerRegistered = false;
        } else if (!timerRegistered) {
            long fireAt = ctx.timerService().currentProcessingTime() + FLUSH_TIMER_INTERVAL_MS;
            ctx.timerService().registerProcessingTimeTimer(fireAt);
            timerRegistered = true;
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichmentResult> out)
            throws Exception {
        flushBuffer(out);
        timerRegistered = false;
    }

    // ── Flush logic ──────────────────────────────────────────────────────

    private void flushBuffer(Collector<EnrichmentResult> out) throws Exception {
        List<RawDocument> batch = drainBuffer();
        if (batch.isEmpty()) {
            return;
        }

        List<String> documentIds = batch.stream()
                .map(RawDocument::getDocumentId)
                .collect(Collectors.toList());
        List<Map<String, Object>> payloads = batch.stream()
                .map(RawDocument::getPayload)
                .collect(Collectors.toList());

        log.debug("Flushing bulk batch of {} documents for enricher '{}'",
                batch.size(), enricherConfig.getName());

        boolean anyReflow = batch.stream().anyMatch(RawDocument::isReflow);

        try {
            List<Map<String, Object>> results = enricher.enrichBulk(documentIds, payloads);
            emitResults(batch, results, out);
        } catch (Exception e) {
            log.warn("Bulk enrichment failed for enricher '{}': {}",
                    enricherConfig.getName(), e.getMessage());

            if (anyReflow && enricherConfig.getMaxRetries() > 0) {
                retryWithRefetch(batch, out, enricherConfig.getMaxRetries());
            } else {
                emitFailures(batch, out, e.getMessage());
            }
        }
    }

    /**
     * Retry with re-fetch: wait, re-fetch documents from ES, and retry the bulk enrichment.
     */
    private void retryWithRefetch(List<RawDocument> batch, Collector<EnrichmentResult> out,
                                  int retriesLeft) {
        try {
            Thread.sleep(enricherConfig.getRetryDelayMs());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            emitFailures(batch, out, "Interrupted during retry delay");
            return;
        }

        List<String> documentIds = batch.stream()
                .map(RawDocument::getDocumentId)
                .collect(Collectors.toList());

        try {
            // Re-fetch documents from ES for fresh data
            List<RawDocument> freshDocs = esService.fetchByIds(esIndex, documentIds);

            if (freshDocs.isEmpty()) {
                log.warn("No documents found on re-fetch for enricher '{}', emitting failures",
                        enricherConfig.getName());
                emitFailures(batch, out, "Documents not found on re-fetch from ES");
                return;
            }

            // Preserve reflow flag and existing enrichments from original batch
            for (RawDocument freshDoc : freshDocs) {
                freshDoc.setReflow(true);
                batch.stream()
                        .filter(orig -> orig.getDocumentId().equals(freshDoc.getDocumentId()))
                        .findFirst()
                        .ifPresent(orig -> freshDoc.setExistingEnrichments(
                                orig.getExistingEnrichments()));
            }

            List<String> freshIds = freshDocs.stream()
                    .map(RawDocument::getDocumentId)
                    .collect(Collectors.toList());
            List<Map<String, Object>> freshPayloads = freshDocs.stream()
                    .map(RawDocument::getPayload)
                    .collect(Collectors.toList());

            List<Map<String, Object>> results = enricher.enrichBulk(freshIds, freshPayloads);
            emitResults(freshDocs, results, out);

        } catch (Exception e) {
            log.warn("Retry {} failed for enricher '{}': {}",
                    enricherConfig.getMaxRetries() - retriesLeft + 1,
                    enricherConfig.getName(), e.getMessage());

            if (retriesLeft > 1) {
                retryWithRefetch(batch, out, retriesLeft - 1);
            } else {
                log.error("All retries exhausted for bulk enricher '{}'", enricherConfig.getName());
                emitFailures(batch, out, e.getMessage());
            }
        }
    }

    // ── Emit helpers ─────────────────────────────────────────────────────

    private void emitResults(List<RawDocument> batch, List<Map<String, Object>> results,
                             Collector<EnrichmentResult> out) {
        for (int i = 0; i < batch.size(); i++) {
            RawDocument doc = batch.get(i);

            if (i < results.size() && results.get(i) != null) {
                out.collect(EnrichmentResult.success(
                        doc.getDocumentId(),
                        enricherConfig.getName(),
                        results.get(i),
                        doc.getPayload(),
                        doc.getBoomerangUpdateCount(),
                        doc.isReflow(),
                        doc.getExistingEnrichments()
                ));
            } else {
                out.collect(EnrichmentResult.failure(
                        doc.getDocumentId(),
                        enricherConfig.getName(),
                        "Bulk enrichment returned null for this document",
                        doc.getPayload(),
                        doc.getBoomerangUpdateCount(),
                        doc.isReflow(),
                        doc.getExistingEnrichments()
                ));
            }
        }
    }

    private void emitFailures(List<RawDocument> batch, Collector<EnrichmentResult> out,
                              String errorMessage) {
        for (RawDocument doc : batch) {
            out.collect(EnrichmentResult.failure(
                    doc.getDocumentId(),
                    enricherConfig.getName(),
                    errorMessage,
                    doc.getPayload(),
                    doc.getBoomerangUpdateCount(),
                    doc.isReflow(),
                    doc.getExistingEnrichments()
            ));
        }
    }

    private List<RawDocument> drainBuffer() throws Exception {
        List<RawDocument> batch = new ArrayList<>();
        for (RawDocument doc : bufferState.get()) {
            batch.add(doc);
        }
        bufferState.clear();
        return batch;
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
