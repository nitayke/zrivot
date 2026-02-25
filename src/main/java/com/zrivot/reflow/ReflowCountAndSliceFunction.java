package com.zrivot.reflow;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.ReflowConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.model.ReflowMessage;
import com.zrivot.model.ReflowSlice;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a {@link ReflowMessage} into one or more {@link ReflowSlice}s.
 *
 * <p>Logic:
 * <ol>
 *   <li>Translates the reflow message query criteria into an ES count query.</li>
 *   <li>If the count exceeds {@code sliceThreshold}, splits into multiple slices.</li>
 *   <li>Otherwise, emits a single unsliced query.</li>
 * </ol>
 *
 * <p>This operator opens and closes its own {@link ElasticsearchService} connection.</p>
 */
public class ReflowCountAndSliceFunction extends ProcessFunction<ReflowMessage, ReflowSlice> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ReflowCountAndSliceFunction.class);

    private final ElasticsearchConfig esConfig;
    private final ReflowConfig reflowConfig;
    private final String index;

    private transient ElasticsearchService esService;

    public ReflowCountAndSliceFunction(ElasticsearchConfig esConfig,
                                       ReflowConfig reflowConfig,
                                       String index) {
        this.esConfig = esConfig;
        this.reflowConfig = reflowConfig;
        this.index = index;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        esService = new ElasticsearchService(esConfig);
    }

    @Override
    public void processElement(ReflowMessage message, Context ctx, Collector<ReflowSlice> out)
            throws Exception {

        var query = message.getQueryCriteria();
        String enricherName = message.getEnricherName();

        try {
            long count = esService.countDocuments(index, query);
            LOG.info("Reflow count for enricher={}: {} documents", enricherName, count);

            if (count == 0) {
                LOG.info("No documents matched for reflow query, skipping. enricher={}", enricherName);
                return;
            }

            if (count > reflowConfig.getSliceThreshold()) {
                // Split into slices for parallel processing
                int numSlices = Math.min(
                        reflowConfig.getMaxSlices(),
                        (int) Math.ceil((double) count / reflowConfig.getSliceThreshold())
                );
                LOG.info("Slicing reflow query into {} slices for enricher={}", numSlices, enricherName);

                for (int i = 0; i < numSlices; i++) {
                    out.collect(new ReflowSlice(enricherName, query, i, numSlices, index));
                }
            } else {
                // Small enough to handle as a single slice
                out.collect(ReflowSlice.unsliced(enricherName, query, index));
            }
        } catch (Exception e) {
            LOG.error("Failed to count documents for reflow enricher={}: {}",
                    enricherName, e.getMessage(), e);
            // Re-throw to let Flink retry (the reflow message will be re-consumed from Kafka)
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (esService != null) {
            esService.close();
        }
        super.close();
    }
}
