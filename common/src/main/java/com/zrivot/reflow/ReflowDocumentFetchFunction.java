package com.zrivot.reflow;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.ReflowConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.elasticsearch.FetchBatchResult;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Fetches documents from Elasticsearch for a given {@link ReflowSlice}, one batch at a time,
 * with full checkpoint recovery support.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>{@code processElement} receives a slice, fetches the <b>first</b> batch synchronously,
 *       stores the {@code search_after} cursor in Flink keyed state, and registers a
 *       processing-time timer for the next batch.</li>
 *   <li>{@code onTimer} fires, reads the cursor from state, fetches the <b>next</b> batch,
 *       updates the cursor, and registers another timer — repeating until all pages are consumed.</li>
 *   <li>Between any two timer fires, Flink can take a <b>checkpoint</b>.  The checkpoint
 *       captures the current {@code search_after} cursor, so after a crash the operator
 *       resumes from exactly where it left off instead of re-fetching everything.</li>
 * </ol>
 *
 * <h3>Why synchronous ES calls?</h3>
 * <p>We intentionally use the <b>synchronous</b> Elasticsearch client.  Each batch must
 * complete, its documents must be emitted, and its cursor must be persisted to Flink state
 * <b>before</b> we move to the next batch.  An async call would decouple the fetch from the
 * checkpoint barrier, making it impossible to guarantee the cursor is consistent with the
 * checkpoint.</p>
 *
 * <p>This operator must be placed after a {@code keyBy(ReflowSlice::getSliceKey)} so that
 * each unique slice has its own isolated state partition.</p>
 */
@Slf4j
public class ReflowDocumentFetchFunction<T>
        extends KeyedProcessFunction<String, ReflowSlice, RawDocument<T>> {

    private static final long serialVersionUID = 3L;

    private final ElasticsearchConfig esConfig;
    private final ReflowConfig reflowConfig;
    private final Class<T> documentClass;

    private transient ElasticsearchService esService;

    // ---- Flink keyed state — checkpointed and restored on crash recovery ----
    /** The slice currently being fetched for this key. */
    private transient ValueState<ReflowSlice> sliceState;
    /** The search_after cursor values from the last completed batch. */
    private transient ListState<String> searchAfterState;
    /** Running total of documents emitted so far (for logging). */
    private transient ValueState<Long> fetchedCountState;

    public ReflowDocumentFetchFunction(ElasticsearchConfig esConfig, ReflowConfig reflowConfig,
                                        Class<T> documentClass) {
        this.esConfig = esConfig;
        this.reflowConfig = reflowConfig;
        this.documentClass = documentClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        esService = new ElasticsearchService(esConfig);

        sliceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("slice", ReflowSlice.class));

        searchAfterState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("searchAfter", Types.STRING));

        fetchedCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("fetchedCount", Types.LONG));
    }

    /**
     * Called when a new {@link ReflowSlice} arrives.  Initialises state and kicks off
     * the first batch fetch.
     */
    @Override
    public void processElement(ReflowSlice slice, Context ctx, Collector<RawDocument<T>> out)
            throws Exception {

        log.info("Starting stateful fetch for {}", slice);

        // Initialise state for the new slice
        sliceState.update(slice);
        searchAfterState.clear();
        fetchedCountState.update(0L);

        // Fetch the first batch immediately (synchronous)
        fetchNextBatch(out, ctx.timerService());
    }

    /**
     * Timer callback — fetches the next batch using the cursor persisted in state.
     * If Flink recovered from a checkpoint, this is where fetching resumes automatically.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RawDocument<T>> out)
            throws Exception {

        ReflowSlice slice = sliceState.value();
        if (slice == null) {
            // State was already cleared (slice completed or cleaned up)
            return;
        }
        fetchNextBatch(out, ctx.timerService());
    }

    // ------------------------------ internal ---------------------------------

    private void fetchNextBatch(Collector<RawDocument<T>> out, TimerService timerService)
            throws Exception {

        ReflowSlice slice = sliceState.value();
        if (slice == null) {
            return;
        }

        // Read the current search_after cursor from state
        List<String> cursor = readSearchAfterCursor();

        // Synchronous ES fetch — one batch at a time for checkpoint safety
        FetchBatchResult<T> result = esService.fetchBatch(
                slice, reflowConfig.getFetchBatchSize(), cursor, documentClass);

        // Emit documents downstream
        for (RawDocument<T> doc : result.getDocuments()) {
            out.collect(doc);
        }

        // Update running total
        long totalFetched = (fetchedCountState.value() != null ? fetchedCountState.value() : 0L)
                + result.getDocuments().size();
        fetchedCountState.update(totalFetched);

        if (result.hasMore()) {
            // Persist the new cursor — this will be part of the next checkpoint
            persistSearchAfterCursor(result.getNextSearchAfter());

            log.debug("Fetched batch of {} docs (total: {}) for {}, scheduling next batch",
                    result.getDocuments().size(), totalFetched, slice);

            // Schedule the next batch via a processing-time timer.
            // Between now and that timer fire, Flink may take a checkpoint — which is
            // the entire point: the checkpoint will include the updated cursor.
            timerService.registerProcessingTimeTimer(
                    timerService.currentProcessingTime() + 1);
        } else {
            // All pages consumed — clean up keyed state
            log.info("Completed fetch for {}. Total documents: {}", slice, totalFetched);
            clearState();
        }
    }

    /** Reads the search_after cursor from {@link ListState}, or returns {@code null} for page 1. */
    private List<String> readSearchAfterCursor() throws Exception {
        List<String> cursor = new ArrayList<>();
        for (String val : searchAfterState.get()) {
            cursor.add(val);
        }
        return cursor.isEmpty() ? null : cursor;
    }

    /** Overwrites the search_after cursor in state with the values from the latest batch. */
    private void persistSearchAfterCursor(List<String> cursorValues) throws Exception {
        searchAfterState.clear();
        for (String val : cursorValues) {
            searchAfterState.add(val);
        }
    }

    /** Clears all keyed state for this slice key — called when fetching is complete. */
    private void clearState() {
        sliceState.clear();
        searchAfterState.clear();
        fetchedCountState.clear();
    }

    @Override
    public void close() throws Exception {
        if (esService != null) {
            esService.close();
        }
        super.close();
    }
}
