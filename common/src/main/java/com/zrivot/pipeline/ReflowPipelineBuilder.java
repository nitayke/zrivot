package com.zrivot.pipeline;

import com.zrivot.config.EnricherConfig;
import com.zrivot.config.PipelineConfig;
import com.zrivot.enrichment.AsyncBulkEnrichmentFunction;
import com.zrivot.enrichment.AsyncEnrichmentFunction;
import com.zrivot.enrichment.BoomerangEnrichmentFunction;
import com.zrivot.kafka.KafkaSourceFactory;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import com.zrivot.reflow.ReflowCountAndSliceFunction;
import com.zrivot.reflow.ReflowDocumentFetchFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Builds the reflow sub-pipeline for a single enricher.
 *
 * <p>Reflow pipeline per enricher:
 * <pre>
 *   [Reflow Kafka Topic]
 *       → ReflowCountAndSlice  (maps query via enricher, count docs, slice)
 *       → keyBy(sliceKey)      → ReflowDocumentFetch (search_after, configurable sort)
 *       → keyBy(documentId)    → BoomerangEnrichmentFunction (keyed state guard)
 *       → AsyncDataStream.orderedWait
 *             single-doc : {@link AsyncEnrichmentFunction}
 *             bulk       : {@link AsyncBulkEnrichmentFunction}
 * </pre>
 *
 * <p>Like {@link RealtimePipelineBuilder}, both single and bulk paths go through
 * {@code AsyncDataStream.orderedWait()} — no {@code keyBy("bulk")}.</p>
 */
@Slf4j
public class ReflowPipelineBuilder {

    private final StreamExecutionEnvironment env;
    private final PipelineConfig config;
    private final KafkaSourceFactory kafkaSourceFactory;

    public ReflowPipelineBuilder(StreamExecutionEnvironment env,
                                 PipelineConfig config,
                                 KafkaSourceFactory kafkaSourceFactory) {
        this.env = env;
        this.config = config;
        this.kafkaSourceFactory = kafkaSourceFactory;
    }

    public DataStream<EnrichmentResult> build(EnricherConfig enricherConfig) {
        String enricherName = enricherConfig.getName();
        log.info("Building reflow pipeline for enricher '{}'", enricherName);

        // 1. Reflow Kafka source
        var reflowSource = kafkaSourceFactory.createReflowSource(
                enricherConfig.getReflowTopic(),
                enricherConfig.getReflowConsumerGroup()
        );
        var reflowMessages = env
                .fromSource(reflowSource, WatermarkStrategy.noWatermarks(),
                        "reflow-source-" + enricherName);

        // 2. Map reflow criteria → ES query, count docs, split into slices
        DataStream<ReflowSlice> slices = reflowMessages
                .process(new ReflowCountAndSliceFunction(
                        config.getElasticsearch(),
                        config.getReflow(),
                        enricherConfig,
                        config.getElasticsearch().getIndex()
                ))
                .name("reflow-count-slice-" + enricherName);

        // 3. Parallel stateful document fetching
        DataStream<RawDocument> reflowDocs = slices
                .keyBy(ReflowSlice::getSliceKey)
                .process(new ReflowDocumentFetchFunction(
                        config.getElasticsearch(),
                        config.getReflow()
                ))
                .name("reflow-fetch-" + enricherName);

        // 4. Boomerang guard
        DataStream<RawDocument> guardedDocs = reflowDocs
                .keyBy(RawDocument::getDocumentId)
                .process(new BoomerangEnrichmentFunction(enricherConfig))
                .name("reflow-guard-" + enricherName);

        // 5. Async enrichment — orderedWait for both bulk and single paths
        if (enricherConfig.isBulkEnabled()) {
            return AsyncDataStream.orderedWait(
                    guardedDocs,
                    new AsyncBulkEnrichmentFunction(
                            enricherConfig,
                            config.getElasticsearch(),
                            config.getElasticsearch().getIndex()),
                    enricherConfig.getAsyncTimeoutMs(),
                    TimeUnit.MILLISECONDS,
                    enricherConfig.getAsyncCapacity()
            ).name("reflow-bulk-enrich-" + enricherName);
        } else {
            return AsyncDataStream.orderedWait(
                    guardedDocs,
                    new AsyncEnrichmentFunction(
                            enricherConfig,
                            config.getElasticsearch(),
                            config.getElasticsearch().getIndex()),
                    enricherConfig.getAsyncTimeoutMs(),
                    TimeUnit.MILLISECONDS,
                    enricherConfig.getAsyncCapacity()
            ).name("reflow-async-enrich-" + enricherName);
        }
    }
}
