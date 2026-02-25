package com.zrivot.joiner;

import com.zrivot.model.EnrichedDocument;
import com.zrivot.model.EnrichmentResult;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Joins enrichment results from all enrichers for the same document.
 *
 * <p>Keyed by documentId, this function:
 * <ol>
 *   <li>Collects {@link EnrichmentResult}s from each enricher in keyed {@link MapState}.</li>
 *   <li>When all expected enrichers have reported (success or failure), emits an
 *       {@link EnrichedDocument} that merges the original payload with all enrichments.</li>
 *   <li>Uses a processing-time timer as a timeout: if not all enrichers report within the
 *       configured window, the document is emitted with whatever enrichments have arrived.</li>
 * </ol>
 *
 * <p>For reflow documents, existing enrichments (from ES) are carried forward if an enricher
 * did not produce a new result.</p>
 */
public class EnrichmentJoinerFunction
        extends KeyedProcessFunction<String, EnrichmentResult, EnrichedDocument> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentJoinerFunction.class);

    private final Set<String> expectedEnricherNames;
    private final long timeoutMs;

    /**
     * State: enricherName → enriched fields map.
     * Null value means the enricher reported a failure.
     */
    private transient MapState<String, Map<String, Object>> enrichmentState;

    /** Stores the original payload from the first result that arrives. */
    private transient ValueState<Map<String, Object>> originalPayloadState;

    /** Stores existing enrichments (from ES) when processing reflow documents. */
    private transient ValueState<Map<String, Map<String, Object>>> existingEnrichmentsState;

    /** Whether a timer has already been set for this key. */
    private transient ValueState<Boolean> timerSetState;

    public EnrichmentJoinerFunction(Set<String> expectedEnricherNames, long timeoutMs) {
        this.expectedEnricherNames = expectedEnricherNames;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        enrichmentState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("enrichments", Types.STRING, Types.MAP(Types.STRING, Types.GENERIC(Object.class)))
        );
        originalPayloadState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("originalPayload", Types.MAP(Types.STRING, Types.GENERIC(Object.class)))
        );
        existingEnrichmentsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("existingEnrichments",
                        Types.MAP(Types.STRING, Types.MAP(Types.STRING, Types.GENERIC(Object.class))))
        );
        timerSetState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerSet", Types.BOOLEAN)
        );
    }

    @Override
    public void processElement(EnrichmentResult result, Context ctx,
                               Collector<EnrichedDocument> out) throws Exception {

        // Store original payload from the first result
        if (originalPayloadState.value() == null) {
            originalPayloadState.update(result.getOriginalPayload());
        }

        // Store existing enrichments for reflow documents
        if (result.isReflow() && result.getExistingEnrichments() != null
                && existingEnrichmentsState.value() == null) {
            existingEnrichmentsState.update(result.getExistingEnrichments());
        }

        // Store the enrichment result (null value for failures)
        if (result.isSuccess()) {
            enrichmentState.put(result.getEnricherName(), result.getEnrichedFields());
        } else {
            enrichmentState.put(result.getEnricherName(), null);
            LOG.warn("Enricher '{}' failed for doc={}: {}",
                    result.getEnricherName(), result.getDocumentId(), result.getErrorMessage());
        }

        // Register timeout timer on first arrival
        Boolean timerSet = timerSetState.value();
        if (timerSet == null || !timerSet) {
            long timerTime = ctx.timerService().currentProcessingTime() + timeoutMs;
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            timerSetState.update(true);
        }

        // Check if all enrichers have reported
        if (allEnrichersReported()) {
            emitAndClear(ctx.getCurrentKey(), out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedDocument> out)
            throws Exception {
        LOG.warn("Joiner timeout for doc={}. Emitting partial enrichment.", ctx.getCurrentKey());
        emitAndClear(ctx.getCurrentKey(), out);
    }

    private boolean allEnrichersReported() throws Exception {
        for (String name : expectedEnricherNames) {
            if (!enrichmentState.contains(name)) {
                return false;
            }
        }
        return true;
    }

    private void emitAndClear(String documentId, Collector<EnrichedDocument> out) throws Exception {
        Map<String, Map<String, Object>> mergedEnrichments = new HashMap<>();

        // Start with existing enrichments (for reflow)
        Map<String, Map<String, Object>> existing = existingEnrichmentsState.value();
        if (existing != null) {
            mergedEnrichments.putAll(existing);
        }

        // Override with new enrichment results (skip failures = null values)
        for (var entry : enrichmentState.entries()) {
            if (entry.getValue() != null) {
                mergedEnrichments.put(entry.getKey(), entry.getValue());
            }
            // If failure and existing enrichment present, the existing one is preserved
        }

        Map<String, Object> originalPayload = originalPayloadState.value();

        EnrichedDocument doc = new EnrichedDocument(
                documentId,
                originalPayload != null ? originalPayload : new HashMap<>(),
                mergedEnrichments,
                System.currentTimeMillis()
        );

        out.collect(doc);
        LOG.debug("Emitted enriched doc={} with {} enrichments", documentId, mergedEnrichments.size());

        // Clear state
        enrichmentState.clear();
        originalPayloadState.clear();
        existingEnrichmentsState.clear();
        timerSetState.clear();
    }
}
