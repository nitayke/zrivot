package com.zrivot.reflow;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.EnricherConfig;
import com.zrivot.config.ReflowConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.enrichment.Enricher;
import com.zrivot.enrichment.EnricherFactory;
import com.zrivot.model.ReflowMessage;
import com.zrivot.model.ReflowSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.UUID;

/**
 * Converts a {@link ReflowMessage} into one or more {@link ReflowSlice}s.
 *
 * <p>Logic:
 * <ol>
 *   <li>Uses the enricher's {@link Enricher#buildReflowQuery} to translate the reflow
 *       message criteria into an Elasticsearch query.</li>
 *   <li>Counts matching documents.</li>
 *   <li>If the count exceeds {@code sliceThreshold}, splits into multiple slices.</li>
 *   <li>Otherwise, emits a single unsliced query.</li>
 * </ol>
 */
@Slf4j
public class ReflowCountAndSliceFunction extends ProcessFunction<ReflowMessage, ReflowSlice> {

    private static final long serialVersionUID = 2L;

    private final ElasticsearchConfig esConfig;
    private final ReflowConfig reflowConfig;
    private final EnricherConfig enricherConfig;
    private final String index;

    private transient ElasticsearchService esService;
    private transient Enricher enricher;

    public ReflowCountAndSliceFunction(ElasticsearchConfig esConfig,
                                       ReflowConfig reflowConfig,
                                       EnricherConfig enricherConfig,
                                       String index) {
        this.esConfig = esConfig;
        this.reflowConfig = reflowConfig;
        this.enricherConfig = enricherConfig;
        this.index = index;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        esService = new ElasticsearchService(esConfig);
        enricher = EnricherFactory.create(enricherConfig);
    }

    @Override
    public void processElement(ReflowMessage message, Context ctx, Collector<ReflowSlice> out)
            throws Exception {

        String enricherName = message.getEnricherName();
        String requestId = UUID.randomUUID().toString();

        // Map reflow criteria to ES query using the enricher's per-topic mapper
        Map<String, Object> esQuery = enricher.buildReflowQuery(message.getQueryCriteria());

        try {
            long count = esService.countDocuments(index, esQuery);
            log.info("Reflow count for enricher={} request={}: {} documents",
                    enricherName, requestId, count);

            if (count == 0) {
                log.info("No documents matched for reflow query, skipping. enricher={}", enricherName);
                return;
            }

            if (count > reflowConfig.getSliceThreshold()) {
                int numSlices = Math.min(
                        reflowConfig.getMaxSlices(),
                        (int) Math.ceil((double) count / reflowConfig.getSliceThreshold())
                );
                log.info("Slicing reflow query into {} slices for enricher={} request={}",
                        numSlices, enricherName, requestId);

                for (int i = 0; i < numSlices; i++) {
                    out.collect(new ReflowSlice(requestId, enricherName, esQuery, i, numSlices, index));
                }
            } else {
                out.collect(ReflowSlice.unsliced(requestId, enricherName, esQuery, index));
            }
        } catch (Exception e) {
            log.error("Failed to count documents for reflow enricher={}: {}",
                    enricherName, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (esService != null) {
            esService.close();
        }
        if (enricher != null) {
            enricher.close();
        }
        super.close();
    }
}
