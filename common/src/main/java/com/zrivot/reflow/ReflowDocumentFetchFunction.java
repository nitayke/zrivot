package com.zrivot.reflow;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.ReflowConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.elasticsearch.FetchBatchResult;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
 * <h3>Sort order</h3>
 * <p>Uses the configurable {@code sortField} from {@link ReflowConfig} to sort by a time
 * field descending (newest first), then by {@code _id}.  This ensures the most recent
 * documents are processed first.</p>
 */
@Slf4j
public class ReflowDocumentFetchFunction
        extends KeyedProcessFunction<String, ReflowSlice, RawDocument> {

    private static final long serialVersionUID = 4L;

    private final ElasticsearchConfig esConfig;
    private final ReflowConfig reflowConfig;

    private transient ElasticsearchService esService;

    // ---- Flink keyed state — checkpointed and restored on crash recovery ----
    private transient ValueState<ReflowSlice> sliceState;
    private transient ListState<String> searchAfterState;
    private transient ValueState<Long> fetchedCountState;

    public ReflowDocumentFetchFunction(ElasticsearchConfig esConfig, ReflowConfig reflowConfig) {
        this.esConfig = esConfig;
        this.reflowConfig = reflowConfig;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        esService = new ElasticsearchService(esConfig);

        sliceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("slice", ReflowSlice.class));

        searchAfterState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("searchAfter", Types.STRING));

        fetchedCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("fetchedCount", Types.LONG));
    }

    @Override
    public void processElement(ReflowSlice slice, Context ctx, Collector<RawDocument> out)
            throws Exception {

        log.info("Starting stateful fetch for {}", slice);

        sliceState.update(slice);
        searchAfterState.clear();
        fetchedCountState.update(0L);

        fetchNextBatch(out, ctx.timerService());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RawDocument> out)
            throws Exception {

        ReflowSlice slice = sliceState.value();
        if (slice == null) {
            return;
        }
        fetchNextBatch(out, ctx.timerService());
    }

    // ------------------------------ internal ---------------------------------

    private void fetchNextBatch(Collector<RawDocument> out, TimerService timerService)
            throws Exception {

        ReflowSlice slice = sliceState.value();
        if (slice == null) {
            return;
        }

        List<String> cursor = readSearchAfterCursor();

        FetchBatchResult result = esService.fetchBatch(
                slice, reflowConfig.getFetchBatchSize(), cursor, reflowConfig.getSortField());

        for (RawDocument doc : result.getDocuments()) {
            out.collect(doc);
        }

        long totalFetched = (fetchedCountState.value() != null ? fetchedCountState.value() : 0L)
                + result.getDocuments().size();
        fetchedCountState.update(totalFetched);

        if (result.hasMore()) {
            persistSearchAfterCursor(result.getNextSearchAfter());

            log.debug("Fetched batch of {} docs (total: {}) for {}, scheduling next batch",
                    result.getDocuments().size(), totalFetched, slice);

            timerService.registerProcessingTimeTimer(
                    timerService.currentProcessingTime() + 1);
        } else {
            log.info("Completed fetch for {}. Total documents: {}", slice, totalFetched);
            clearState();
        }
    }

    private List<String> readSearchAfterCursor() throws Exception {
        List<String> cursor = new ArrayList<>();
        for (String val : searchAfterState.get()) {
            cursor.add(val);
        }
        return cursor.isEmpty() ? null : cursor;
    }

    private void persistSearchAfterCursor(List<String> cursorValues) throws Exception {
        searchAfterState.clear();
        for (String val : cursorValues) {
            searchAfterState.add(val);
        }
    }

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
