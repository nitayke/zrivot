package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink keyed process function that guards enrichment execution with a boomerang update count check.
 *
 * <p>For each document (keyed by documentId):
 * <ol>
 *   <li>Checks if the incoming {@code boomerangUpdateCount} is greater than or equal to the
 *       stored state count. If not, the event is stale and is dropped.</li>
 *   <li>Delegates to the configured {@link Enricher} implementation.</li>
 *   <li>On success, emits an {@link EnrichmentResult} with the enriched fields.</li>
 *   <li>On failure, emits a failed {@link EnrichmentResult} so that the joiner can still proceed
 *       (one enricher failure does NOT block others).</li>
 * </ol>
 */
@Slf4j
public class BoomerangEnrichmentFunction
        extends KeyedProcessFunction<String, RawDocument, EnrichmentResult> {

    private static final long serialVersionUID = 1L;

    private final EnricherConfig enricherConfig;

    private transient Enricher enricher;
    private transient ValueState<Long> updateCountState;

    public BoomerangEnrichmentFunction(EnricherConfig enricherConfig) {
        this.enricherConfig = enricherConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialise the enricher via factory
        this.enricher = EnricherFactory.create(enricherConfig);

        // Register keyed state for the boomerang update counter
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "boomerangUpdateCount-" + enricherConfig.getName(),
                Types.LONG
        );
        this.updateCountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(RawDocument doc, Context ctx, Collector<EnrichmentResult> out)
            throws Exception {

        String documentId = doc.getDocumentId();
        long incomingCount = doc.getBoomerangUpdateCount();

        // ── Boomerang guard ──────────────────────────────────────────────
        Long currentCount = updateCountState.value();
        if (currentCount != null && incomingCount < currentCount) {
            log.debug("Dropping stale event for doc={} enricher={}: incoming={} < state={}",
                    documentId, enricherConfig.getName(), incomingCount, currentCount);
            return;
        }
        updateCountState.update(incomingCount);

        // ── Execute enrichment (isolated: failure doesn't propagate) ─────
        try {
            var enrichedFields = enricher.enrich(documentId, doc.getPayload());

            out.collect(EnrichmentResult.success(
                    documentId,
                    enricherConfig.getName(),
                    enrichedFields,
                    doc.getPayload(),
                    incomingCount,
                    doc.isReflow(),
                    doc.getExistingEnrichments()
            ));

            log.debug("Enrichment success: doc={} enricher={}", documentId, enricherConfig.getName());
        } catch (Exception e) {
            log.error("Enrichment failed for doc={} enricher={}: {}",
                    documentId, enricherConfig.getName(), e.getMessage(), e);

            out.collect(EnrichmentResult.failure(
                    documentId,
                    enricherConfig.getName(),
                    e.getMessage(),
                    doc.getPayload(),
                    incomingCount,
                    doc.isReflow(),
                    doc.getExistingEnrichments()
            ));
        }
    }

    @Override
    public void close() throws Exception {
        if (enricher != null) {
            enricher.close();
        }
        super.close();
    }
}
