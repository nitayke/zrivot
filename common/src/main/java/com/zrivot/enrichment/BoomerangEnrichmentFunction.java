package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink keyed process function that implements the boomerang update-count guard.
 *
 * <p>For each document (keyed by documentId):
 * <ol>
 *   <li>Checks if the incoming {@code boomerangUpdateCount} is greater than or equal to the
 *       stored state count.  If not, the event is stale and is dropped.</li>
 *   <li>On pass, emits the {@link RawDocument} downstream for asynchronous enrichment.</li>
 * </ol>
 *
 * <p>This function uses <b>keyed state</b> (which is not available inside a
 * {@code RichAsyncFunction}), so the guard must be a separate operator placed
 * <b>before</b> the async enrichment step.</p>
 */
@Slf4j
public class BoomerangEnrichmentFunction
        extends KeyedProcessFunction<String, RawDocument<?>, RawDocument<?>> {

    private static final long serialVersionUID = 3L;

    private final EnricherConfig enricherConfig;

    private transient ValueState<Long> updateCountState;

    public BoomerangEnrichmentFunction(EnricherConfig enricherConfig) {
        this.enricherConfig = enricherConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "boomerangUpdateCount-" + enricherConfig.getName(),
                Types.LONG
        );
        this.updateCountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(RawDocument<?> doc, Context ctx, Collector<RawDocument<?>> out)
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

        // Pass through — enrichment happens asynchronously in the next operator
        out.collect(doc);
    }
}
