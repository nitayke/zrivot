package com.zrivot.reflow;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.config.ReflowConfig;
import com.zrivot.elasticsearch.ElasticsearchService;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Fetches documents from Elasticsearch for a given {@link ReflowSlice}.
 *
 * <p>Each slice (potentially one of many for a large query) is processed independently.
 * This operator is designed to run after a {@code keyBy(slice.sliceId)} so that slices
 * are distributed across Flink task slots for parallel retrieval.</p>
 *
 * <p>On failure, the exception is thrown (not swallowed), which causes Flink to retry.
 * This is the correct behaviour for reflow: the slice can be re-fetched from Elasticsearch.</p>
 */
public class ReflowDocumentFetchFunction extends ProcessFunction<ReflowSlice, RawDocument> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ReflowDocumentFetchFunction.class);

    private final ElasticsearchConfig esConfig;
    private final ReflowConfig reflowConfig;

    private transient ElasticsearchService esService;

    public ReflowDocumentFetchFunction(ElasticsearchConfig esConfig, ReflowConfig reflowConfig) {
        this.esConfig = esConfig;
        this.reflowConfig = reflowConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        esService = new ElasticsearchService(esConfig);
    }

    @Override
    public void processElement(ReflowSlice slice, Context ctx, Collector<RawDocument> out)
            throws Exception {

        LOG.info("Fetching documents for {}", slice);

        try {
            List<RawDocument> documents = esService.fetchDocuments(slice, reflowConfig.getFetchBatchSize());

            for (RawDocument doc : documents) {
                out.collect(doc);
            }

            LOG.info("Emitted {} documents from {}", documents.size(), slice);
        } catch (Exception e) {
            LOG.error("Failed to fetch documents for {}: {}", slice, e.getMessage(), e);
            // Re-throw to trigger Flink retry — the data is still in ES
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
